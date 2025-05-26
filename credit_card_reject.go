package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// UserFlowResult represents the initial query result from card_statuses
type UserFlowResult struct {
	MobileNumber string    `json:"mobile_number"`
	Reasons      string    `json:"reasons"`
	CreatedAt    time.Time `json:"created_at"`
}

// UserDetails represents the user data from the users table
type UserDetails struct {
	ID                uint32
	FullName          string
	MobileNumber      string // For mapping with card_statuses.mobile_number
	PlainMobileNumber string
}

// CustomHeaderDetails represents the data from custom_headers
type CustomHeaderDetails struct {
	XPlatform    string
	XDeviceToken string
}

// NotificationStatusDetails represents the data from notification_status
type NotificationStatusDetails struct {
	EventName string
	Attempt   int
}

// NotificationConfigDetails represents the data from notification_config
type NotificationConfigDetails struct {
	Delay     int // Delay in seconds
	Channel   string
	EventName string
	EventID   int
}

// Notification represents the final struct to print
type Notification struct {
	Event         string            `json:"event"`
	Delay         float64           `json:"delay"`
	UserID        uint32            `json:"user_id"`
	Mobile        string            `json:"mobile"`
	PlainMobile   string            `json:"plain_mobile"`
	CurrentStatus string            `json:"current_status"`
	Attempt       int               `json:"attempt"`
	Source        string            `json:"source"`
	Channel       string            `json:"channel"`
	Metadata      map[string]string `json:"metadata"`
	DeviceToken   string            `json:"device_token"`
	EventID       int               `json:"event_id"`
}

// UserFlowWithEvent combines user flow data with event type
type UserFlowWithEvent struct {
	UserFlow  UserFlowResult
	EventType string
}

// connectDB establishes a connection to the PostgreSQL database
func connectDB() (*gorm.DB, error) {
	err := godotenv.Load()
	if err != nil {
		log.Printf("No .env file found, relying on system environment variables")
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, fmt.Errorf("DATABASE_URL must be set in environment variables")
	}

	db, err := gorm.Open(postgres.Open(dbURL), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info), // Enable query logging
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	return db, nil
}

// fetchCreditCardRejectedUsers retrieves users with DECLINED status in card_statuses
func fetchCreditCardRejectedUsers(db *gorm.DB, batchSize int) ([]UserFlowWithEvent, error) {
	var allUsers []UserFlowWithEvent
	offset := 0

	// Configurable lookback period (default 7 days)
	lookbackDays := 7
	if days := os.Getenv("LOOKBACK_DAYS"); days != "" {
		if n, err := fmt.Sscanf(days, "%d", &lookbackDays); err != nil || n != 1 {
			log.Printf("Invalid LOOKBACK_DAYS=%s, using default 7 days", days)
		}
	}
	lookbackInterval := fmt.Sprintf("%d day", lookbackDays)
	log.Printf("Fetching CREDIT_CARD_REJECTED users with lookback interval: %s", lookbackInterval)

	// Log the start of the time range
	startTime := time.Now().Add(-time.Duration(lookbackDays) * 24 * time.Hour)
	log.Printf("Querying card_statuses since %s", startTime.Format(time.RFC3339))

	for {
		var users []struct {
			MobileNumber string
			Reasons      string
			CreatedAt    time.Time
		}
		query := fmt.Sprintf(`
			SELECT DISTINCT cs.mobile_number, cs.created_at
			FROM card_statuses cs
			LEFT JOIN users u ON cs.mobile_number = u.mobile_number
			WHERE cs.status = 'DECLINED'
			  AND cs.created_at >= NOW() - INTERVAL '%s'
			LIMIT ? OFFSET ?
		`, lookbackInterval)
		err := db.Raw(query, batchSize, offset).Scan(&users).Error
		if err != nil {
			log.Printf("Error fetching CREDIT_CARD_REJECTED users at offset %d: %v", offset, err)
			return nil, fmt.Errorf("error fetching CREDIT_CARD_REJECTED users at offset %d: %v", offset, err)
		}

		for _, user := range users {
			log.Printf("Fetched record: mobile_number=%s, reasons=%s, created_at=%s",
				user.MobileNumber, user.Reasons, user.CreatedAt.Format(time.RFC3339))
			if user.MobileNumber == "" {
				log.Printf("Warning: No matching user found for mobile_number=%s in users table", user.MobileNumber)
				continue
			}
			allUsers = append(allUsers, UserFlowWithEvent{
				UserFlow: UserFlowResult{
					MobileNumber: user.MobileNumber,
					Reasons:      user.Reasons,
					CreatedAt:    user.CreatedAt,
				},
				EventType: "CREDIT_CARD_REJECTED",
			})
		}

		log.Printf("Fetched batch of CREDIT_CARD_REJECTED users: batchSize=%d, offset=%d, totalFetched=%d", len(users), offset, len(allUsers))
		if len(users) < batchSize {
			break
		}
		offset += batchSize
	}

	if len(allUsers) == 0 {
		log.Printf("No users found in card_statuses with DECLINED status for the last %d days or no matching users in users table. Please verify data or adjust LOOKBACK_DAYS.", lookbackDays)
	}

	return allUsers, nil
}

// fetchUserDetails retrieves user details for multiple mobile numbers
func fetchUserDetails(db *gorm.DB, mobileNumbers []string) (map[string]UserDetails, error) {
	var userDetails []UserDetails
	log.Printf("Querying users table for mobile numbers: %v", mobileNumbers)
	err := db.Table("users").
		Select("id, full_name, mobile_number, plain_mobile_number").
		Where("mobile_number IN ?", mobileNumbers).
		Scan(&userDetails).Error
	if err != nil {
		log.Printf("Error fetching user details for %d mobile numbers: %v", len(mobileNumbers), err)
		return nil, fmt.Errorf("error fetching user details: %v", err)
	}

	userDetailsMap := make(map[string]UserDetails)
	for _, detail := range userDetails {
		log.Printf("Found user: mobile_number=%s, id=%d, plain_mobile_number=%s", detail.MobileNumber, detail.ID, detail.PlainMobileNumber)
		userDetailsMap[detail.MobileNumber] = detail
	}
	if len(userDetails) == 0 {
		log.Printf("No users found for provided mobile numbers")
	}
	return userDetailsMap, nil
}

// fetchCustomHeader retrieves custom headers for multiple user IDs
func fetchCustomHeader(db *gorm.DB, userIDs []uint32) (map[uint32]CustomHeaderDetails, error) {
	var customHeaders []struct {
		UserID       uint32
		XPlatform    string
		XDeviceToken string
	}
	log.Printf("Querying custom_headers for user IDs: %v", userIDs)
	err := db.Table("custom_headers").
		Select("user_id, x_platform, x_device_token").
		Where("user_id IN ?", userIDs).
		Order("user_id, updated_at DESC").
		Scan(&customHeaders).Error
	if err != nil {
		log.Printf("Error fetching custom headers for %d user IDs: %v", len(userIDs), err)
		return nil, fmt.Errorf("error fetching custom headers: %v", err)
	}

	customHeadersMap := make(map[uint32]CustomHeaderDetails)
	for _, header := range customHeaders {
		if _, exists := customHeadersMap[header.UserID]; !exists {
			customHeadersMap[header.UserID] = CustomHeaderDetails{
				XPlatform:    header.XPlatform,
				XDeviceToken: header.XDeviceToken,
			}
		}
	}
	if len(customHeaders) == 0 {
		log.Printf("No custom headers found for provided user IDs")
	}
	return customHeadersMap, nil
}

// fetchNotificationStatus retrieves the latest notification status for a user and event
func fetchNotificationStatus(db *gorm.DB, userID uint32, eventName string) (NotificationStatusDetails, error) {
	var notificationStatus NotificationStatusDetails
	err := db.Table("notification_status").
		Select("event_name, attempt").
		Where("user_id = ? AND event_name = ?", userID, eventName).
		Order("updated_at DESC").
		Limit(1).
		Scan(&notificationStatus).Error
	if err != nil {
		log.Printf("Error fetching notification status for user_id %d, event %s: %v", userID, eventName, err)
		return NotificationStatusDetails{}, fmt.Errorf("error fetching notification status for user_id %d, event %s: %v", userID, eventName, err)
	}
	return notificationStatus, nil
}

// fetchNotificationConfig retrieves notification config for an event and attempt
func fetchNotificationConfig(db *gorm.DB, eventName string, attempt int) (NotificationConfigDetails, error) {
	var notificationConfig NotificationConfigDetails
	err := db.Table("notification_config").
		Select("delay, channel, event_name, event_id").
		Where("event_name = ? AND attempt = ?", eventName, attempt).
		Limit(1).
		Scan(&notificationConfig).Error
	if err != nil {
		log.Printf("Error querying notification config for event %s, attempt %d: %v", eventName, attempt, err)
		return NotificationConfigDetails{}, err
	}
	if notificationConfig.EventName == "" {
		log.Printf("No notification config found for event %s, attempt %d", eventName, attempt)
	} else {
		log.Printf("Found notification config for event %s, attempt %d: delay=%d, channel=%s, event_id=%d",
			notificationConfig.EventName, attempt, notificationConfig.Delay, notificationConfig.Channel, notificationConfig.EventID)
	}
	return notificationConfig, nil
}

// buildNotification constructs a Notification struct with new_delay logic
func buildNotification(userFlow UserFlowResult, userDetail UserDetails, customHeader CustomHeaderDetails, notificationConfig NotificationConfigDetails, attempt int, eventName string) Notification {
	source := os.Getenv("SOURCE")
	if source == "" {
		source = "legacy credit card rejected default"
	}

	// Calculate scheduled_time = created_at + delay (in seconds)
	scheduledTime := userFlow.CreatedAt.Add(time.Duration(notificationConfig.Delay) * time.Second)

	// Calculate new_delay = scheduled_time - current_time (in seconds, with fractional seconds)
	currentTime := time.Now()
	newDelay := scheduledTime.Sub(currentTime).Seconds()

	// Log for debugging
	log.Printf("user_id %d, event %s: created_at=%s, scheduledTime=%s, delay=%d seconds, newDelay=%.2f seconds",
		userDetail.ID, eventName, userFlow.CreatedAt.Format(time.RFC3339), scheduledTime.Format(time.RFC3339), notificationConfig.Delay, newDelay)

	// Skip notifications with negative delay (past-due)
	if newDelay < 0 {
		log.Printf("Skipping notification for user_id %d, event %s: negative delay (%.2f seconds)", userDetail.ID, eventName, newDelay)
		return Notification{}
	}

	return Notification{
		Event:         notificationConfig.EventName,
		Delay:         newDelay,
		UserID:        userDetail.ID,
		Mobile:        userFlow.MobileNumber,
		PlainMobile:   userDetail.PlainMobileNumber,
		CurrentStatus: "DECLINED", // Hardcoded since event is based on DECLINED status
		Attempt:       attempt,
		Source:        source,
		Channel:       notificationConfig.Channel,
		Metadata: map[string]string{
			"Name":    userDetail.FullName,
			"Reasons": userFlow.Reasons,
		},
		DeviceToken: customHeader.XDeviceToken,
		EventID:     notificationConfig.EventID,
	}
}

// printNotifications outputs the notifications in a formatted way
func printNotifications(notifications []Notification) {
	count := 0
	for _, notification := range notifications {
		// Skip empty notifications (e.g., those with negative delays)
		if notification.Event == "" {
			continue
		}
		fmt.Printf("Notification:\n")
		fmt.Printf("  Event: %s\n", notification.Event)
		fmt.Printf("  Delay (seconds): %.2f\n", notification.Delay)
		fmt.Printf("  UserID: %d\n", notification.UserID)
		fmt.Printf("  Mobile: %s\n", notification.Mobile)
		fmt.Printf("  PlainMobile: %s\n", notification.PlainMobile)
		fmt.Printf("  CurrentStatus: %s\n", notification.CurrentStatus)
		fmt.Printf("  Attempt: %d\n", notification.Attempt)
		fmt.Printf("  Source: %s\n", notification.Source)
		fmt.Printf("  Channel: %s\n", notification.Channel)
		fmt.Printf("  Metadata: {Name: %s, Reasons: %s}\n", notification.Metadata["Name"], notification.Metadata["Reasons"])
		fmt.Printf("  DeviceToken: %s\n", notification.DeviceToken)
		fmt.Printf("  EventID: %d\n", notification.EventID)
		fmt.Printf("\n")
		count++
	}
	log.Printf("Printed notifications: total=%d", count)
}

func main() {
	// Initialize standard logger
	logger := log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime)

	// Connect to database
	db, err := connectDB()
	if err != nil {
		logger.Printf("Error connecting to database: %v", err)
		os.Exit(1)
	}

	const batchSize = 1000 // Configurable batch size

	// Fetch users for CREDIT_CARD_REJECTED event
	allUsers, err := fetchCreditCardRejectedUsers(db, batchSize)
	if err != nil {
		logger.Printf("Error fetching CREDIT_CARD_REJECTED users: %v", err)
		os.Exit(1)
	}
	logger.Printf("Fetched CREDIT_CARD_REJECTED users: total=%d", len(allUsers))

	// Skip further processing if no users found
	if len(allUsers) == 0 {
		logger.Printf("No users to process, exiting")
		return
	}

	// Collect mobile numbers and user IDs for batch fetching
	mobileNumbers := make([]string, 0, len(allUsers))
	processedMobileNumbers := make(map[string]struct{})
	for _, user := range allUsers {
		if _, exists := processedMobileNumbers[user.UserFlow.MobileNumber]; !exists {
			mobileNumbers = append(mobileNumbers, user.UserFlow.MobileNumber)
			processedMobileNumbers[user.UserFlow.MobileNumber] = struct{}{}
		}
	}

	// Batch fetch user details
	userDetailsMap, err := fetchUserDetails(db, mobileNumbers)
	if err != nil {
		logger.Printf("Error fetching user details: %v", err)
		os.Exit(1)
	}

	// Collect user IDs for custom headers
	userIDs := make([]uint32, 0, len(userDetailsMap))
	for _, detail := range userDetailsMap {
		if detail.ID != 0 {
			userIDs = append(userIDs, detail.ID)
		}
	}

	// Batch fetch custom headers
	customHeadersMap, err := fetchCustomHeader(db, userIDs)
	if err != nil {
		logger.Printf("Error fetching custom headers: %v", err)
		os.Exit(1)
	}

	// Process users and build notifications
	var notifications []Notification
	var errs []error
	for _, userWithEvent := range allUsers {
		userFlow := userWithEvent.UserFlow
		eventName := userWithEvent.EventType

		// Get user details from map
		userDetail, exists := userDetailsMap[userFlow.MobileNumber]
		if !exists || userDetail.ID == 0 {
			logger.Printf("No user found for mobile number %s", userFlow.MobileNumber)
			continue
		}

		// Get custom header from map
		customHeader, exists := customHeadersMap[userDetail.ID]
		if !exists {
			customHeader = CustomHeaderDetails{XPlatform: "Unknown", XDeviceToken: "Not Available"}
		}

		// Fetch notification status
		notificationStatus, err := fetchNotificationStatus(db, userDetail.ID, eventName)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		attempt := 1
		if notificationStatus.EventName != "" {
			attempt = notificationStatus.Attempt + 1
		}

		// Fetch notification config
		notificationConfig, err := fetchNotificationConfig(db, eventName, attempt)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if notificationConfig.EventName == "" {
			logger.Printf("No valid notification config for user_id %d, event %s, attempt %d, skipping", userDetail.ID, eventName, attempt)
			continue
		}

		// Build and collect notification
		notification := buildNotification(userFlow, userDetail, customHeader, notificationConfig, attempt, eventName)
		notifications = append(notifications, notification)
	}

	// Log the number of unique mobile numbers
	logger.Printf("Total unique mobile numbers after deduplication: %d", len(processedMobileNumbers))

	// Print notifications
	printNotifications(notifications)

	// Report aggregated errors
	if len(errs) > 0 {
		logger.Printf("Encountered %d errors during processing:", len(errs))
		for i, err := range errs {
			logger.Printf("Error %d: %v", i+1, err)
		}
	}
}
