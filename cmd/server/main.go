package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	
	"supabase-manager/internal/api"
	"supabase-manager/internal/storage"
	"supabase-manager/internal/supabase"
)

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	// Get configuration from environment
	config := loadConfig()

	// Validate required configuration
	if err := config.Validate(); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	// Initialize storage
	log.Println("Initializing storage...")
	store, err := storage.NewSQLiteStorage(config.DBPath)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	// Initialize Supabase client
	log.Println("Initializing Supabase client...")
	supabaseClient := supabase.NewClient(
		config.SupabaseAccessToken,
		config.SupabaseOrgID,
	)

	// Test Supabase connection
	if err := supabaseClient.TestConnection(); err != nil {
		log.Printf("Warning: Cannot connect to Supabase API: %v", err)
		log.Println("Server will start but project creation will fail")
	} else {
		log.Println("✓ Successfully connected to Supabase API")
	}

	// Initialize handlers
	handler := api.NewHandler(supabaseClient, store, config.DefaultRegion)

	// Setup router
	router := setupRouter(handler, config)

	// temp code

	router.GET("/debug/routes", func(c *gin.Context) {
	c.JSON(200, gin.H{
		"routes": router.Routes(),
			})
		})


	// Start server
	addr := fmt.Sprintf(":%s", config.Port)
	log.Printf("Starting server on %s", addr)
	log.Printf("Health check: http://localhost%s/health", addr)
	log.Printf("API base URL: http://localhost%s/api", addr)

	// Graceful shutdown
	go func() {
		if err := router.Run(addr); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	
	// Wait for background tasks to finish
	log.Println("Waiting for background tasks...")
	handler.WaitForPendingTasks()
	log.Println("Server shutdown complete.")
}

// Config holds application configuration
type Config struct {
	Port                 string
	DBPath               string
	SupabaseAccessToken  string
	SupabaseOrgID        string
	APIKey               string
	DefaultRegion        string
	LogLevel             string
}

// loadConfig loads configuration from environment variables
func loadConfig() *Config {
	return &Config{
		Port:                 getEnv("PORT", "8080"),
		DBPath:               getEnv("DB_PATH","/tmp/supabase-manager.db"),
		SupabaseAccessToken:  getEnv("SUPABASE_ACCESS_TOKEN", ""),
		SupabaseOrgID:        getEnv("SUPABASE_ORGANIZATION_ID", ""),
		APIKey:               getEnv("API_KEY", "dev-api-key-change-in-production"),
		DefaultRegion:        getEnv("DEFAULT_REGION", "us-east-1"),
		LogLevel:             getEnv("LOG_LEVEL", "info"),
	}
}

// Validate checks if required configuration is present
func (c *Config) Validate() error {
	if c.SupabaseAccessToken == "" {
		return fmt.Errorf("SUPABASE_ACCESS_TOKEN is required")
	}
	if c.SupabaseOrgID == "" {
		return fmt.Errorf("SUPABASE_ORGANIZATION_ID is required")
	}
	return nil
}

// setupRouter configures the HTTP router
func setupRouter(handler *api.Handler, config *Config) *gin.Engine {
	// Set Gin mode based on log level
	if config.LogLevel == "debug" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.Default()

	// CORS middleware
	router.Use(corsMiddleware())

	// Public routes
	router.GET("/health", handler.HealthCheck)

	// API routes (with authentication)
	apiRoutes := router.Group("/api")
	apiRoutes.Use(authMiddleware(config.APIKey))
	{
		// Projects
		apiRoutes.POST("/projects", handler.CreateProject)
		apiRoutes.GET("/projects", handler.ListProjects)
		apiRoutes.GET("/projects/:id", handler.GetProject)
		apiRoutes.DELETE("/projects/:id", handler.DeleteProject)

		// Schema management
		apiRoutes.POST("/projects/:id/schema", handler.ApplySchema)

		// Statistics
		apiRoutes.GET("/stats", handler.GetStats)
	}

	return router
}

// corsMiddleware adds CORS headers
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

// authMiddleware validates API key
func authMiddleware(validAPIKey string) gin.HandlerFunc {
	return func(c *gin.Context) {
		apiKey := c.GetHeader("X-API-Key")
		
		if apiKey == "" {
			c.JSON(401, gin.H{
				"error": gin.H{
					"code":    "UNAUTHORIZED",
					"message": "API key required",
				},
			})
			c.Abort()
			return
		}

		if apiKey != validAPIKey {
			c.JSON(401, gin.H{
				"error": gin.H{
					"code":    "UNAUTHORIZED",
					"message": "Invalid API key",
				},
			})
			c.Abort()
			return
		}

		c.Next()
	}
}

// getEnv gets environment variable with default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}