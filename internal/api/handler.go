package api

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	
	"supabase-manager/internal/storage"
	"supabase-manager/internal/supabase"
)

// Handler contains dependencies for HTTP handlers
type Handler struct {
	supabaseClient *supabase.Client
	storage        *storage.SQLiteStorage
	wg             sync.WaitGroup
	defaultRegion  string
}

// NewHandler creates a new handler instance
func NewHandler(supabaseClient *supabase.Client, storage *storage.SQLiteStorage, defaultRegion string) *Handler {
	return &Handler{
		supabaseClient: supabaseClient,
		storage:        storage,
		defaultRegion:  defaultRegion,
	}
}

// WaitForPendingTasks waits for all background tasks to complete
func (h *Handler) WaitForPendingTasks() {
	h.wg.Wait()
}

// HealthCheck handles GET /health
func (h *Handler) HealthCheck(c *gin.Context) {
	// Test database connection
	dbStatus := "connected"
	if _, err := h.storage.GetStats(); err != nil {
		dbStatus = "error"
	}

	// Test Supabase API
	supabaseStatus := "reachable"
	if err := h.supabaseClient.TestConnection(); err != nil {
		supabaseStatus = "error"
	}

	c.JSON(http.StatusOK, gin.H{
		"status":       "ok",
		"database":     dbStatus,
		"supabase_api": supabaseStatus,
		"timestamp":    time.Now().Format(time.RFC3339),
	})
}

// CreateProject handles POST /api/projects
func (h *Handler) CreateProject(c *gin.Context) {
	var req supabase.CreateProjectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, supabase.ErrorResponse{
			Error: supabase.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "Invalid request body",
				Details: err.Error(),
			},
		})
		return
	}

	// Set default region if not provided
	if req.Region == "" {
		req.Region = h.defaultRegion
	}

	// Generate unique project name if needed
	projectName := req.Name
	if projectName == "" {
		projectName = fmt.Sprintf("project-%s", uuid.New().String()[:8])
	}

	// Create project via Supabase API
	project, err := h.supabaseClient.CreateProject(projectName, req.Region)
	if err != nil {
		c.JSON(http.StatusInternalServerError, supabase.ErrorResponse{
			Error: supabase.ErrorDetail{
				Code:    "PROJECT_CREATION_FAILED",
				Message: "Failed to create Supabase project",
				Details: err.Error(),
			},
		})
		return
	}

	// Generate a stable ID for our system
	projectID := uuid.New().String()
	project.ID = projectID
	project.Region = req.Region // Store the region we used

	// Store initial project data (status will be updated later)
	storedProject := project.ToStoredProject()
	if err := h.storage.SaveProject(storedProject); err != nil {
		// Project created in Supabase but failed to save locally
		// Log error but don't fail the request
		fmt.Printf("Warning: Failed to save project to storage: %v\n", err)
	}

	// Start waiting for project in background
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		
		readyProject, err := h.supabaseClient.WaitForProject(project.ProjectRef, 5*time.Minute)
		if err != nil {
			fmt.Printf("Error waiting for project %s: %v\n", projectID, err)
			h.storage.UpdateProjectStatus(projectID, "FAILED")
			return
		}

		// Fetch API keys from Supabase
		apiKeys, err := h.supabaseClient.GetProjectAPIKeys(project.ProjectRef)
		if err != nil {
			fmt.Printf("Error fetching API keys for %s: %v\n", projectID, err)
			// Still mark as active even if we can't get keys right away
			// They might be available later
		}

		// Update with full details once ready
		readyProject.ID = projectID
		readyProject.Region = req.Region
		readyProject.DBPassword = project.DBPassword // Preserve the password we generated
		
		updatedStoredProject := readyProject.ToStoredProject()

		// Store API keys if we got them
		if apiKeys != nil {
			updatedStoredProject.AnonKey = apiKeys.AnonKey
			updatedStoredProject.ServiceKey = apiKeys.ServiceKey
		}

		if err := h.storage.SaveProject(updatedStoredProject); err != nil {
			fmt.Printf("Error updating project %s: %v\n", projectID, err)
		}
	}()

	c.JSON(http.StatusCreated, gin.H{
		"id":          projectID,
		"project_ref": project.ProjectRef,
		"project_url": project.GetProjectURL(),
		"status":      "creating",
		"message":     "Project creation initiated. Poll /api/projects/:id to check status.",
	})
}

// GetProject handles GET /api/projects/:id
func (h *Handler) GetProject(c *gin.Context) {
	projectID := c.Param("id")

	project, err := h.storage.GetProject(projectID)
	if err != nil {
		c.JSON(http.StatusNotFound, supabase.ErrorResponse{
			Error: supabase.ErrorDetail{
				Code:    "PROJECT_NOT_FOUND",
				Message: "Project not found",
				Details: err.Error(),
			},
		})
		return
	}

	// Return project with sensitive data (service key) only if requested
	response := gin.H{
		"id":          project.ID,
		"project_ref": project.ProjectRef,
		"project_url": project.ProjectURL,
		"anon_key":    project.AnonKey,
		"status":      project.Status,
		"created_at":  project.CreatedAt,
		"updated_at":  project.UpdatedAt,
	}

	// Include sensitive keys if query param is set
	if c.Query("include_keys") == "true" {
		response["service_key"] = project.ServiceKey
		response["db_password"] = project.DBPassword
		response["region"] = project.Region
	}

	c.JSON(http.StatusOK, response)
}

// ListProjects handles GET /api/projects
func (h *Handler) ListProjects(c *gin.Context) {
	projects, err := h.storage.ListProjects()
	if err != nil {
		c.JSON(http.StatusInternalServerError, supabase.ErrorResponse{
			Error: supabase.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to list projects",
				Details: err.Error(),
			},
		})
		return
	}

	// Return simplified list (without sensitive keys)
	var projectList []gin.H
	for _, p := range projects {
		projectList = append(projectList, gin.H{
			"id":          p.ID,
			"project_ref": p.ProjectRef,
			"project_url": p.ProjectURL,
			"status":      p.Status,
			"created_at":  p.CreatedAt,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"projects": projectList,
		"total":    len(projectList),
	})
}

// ApplySchema handles POST /api/projects/:id/schema
func (h *Handler) ApplySchema(c *gin.Context) {
	projectID := c.Param("id")

	var req supabase.ApplySchemaRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, supabase.ErrorResponse{
			Error: supabase.ErrorDetail{
				Code:    "INVALID_REQUEST",
				Message: "Invalid request body",
				Details: err.Error(),
			},
		})
		return
	}

	// Get project from storage
	storedProject, err := h.storage.GetProject(projectID)
	if err != nil {
		c.JSON(http.StatusNotFound, supabase.ErrorResponse{
			Error: supabase.ErrorDetail{
				Code:    "PROJECT_NOT_FOUND",
				Message: "Project not found",
				Details: err.Error(),
			},
		})
		return
	}

	// Check if project is ready
	if storedProject.Status != "ACTIVE_HEALTHY" {
		c.JSON(http.StatusBadRequest, supabase.ErrorResponse{
			Error: supabase.ErrorDetail{
				Code:    "PROJECT_NOT_READY",
				Message: "Project is not ready yet",
				Details: fmt.Sprintf("Current status: %s", storedProject.Status),
			},
		})
		return
	}

	// Convert to supabase.Project for migration runner
	project := &supabase.Project{
		ProjectRef: storedProject.ProjectRef,
		DBPassword: storedProject.DBPassword,
		Region:     storedProject.Region,
	}

	// Create migration runner
	runner, err := supabase.NewMigrationRunner(project)
	if err != nil {
		c.JSON(http.StatusInternalServerError, supabase.ErrorResponse{
			Error: supabase.ErrorDetail{
				Code:    "MIGRATION_FAILED",
				Message: "Failed to connect to database",
				Details: err.Error(),
			},
		})
		return
	}
	defer runner.Close()

	// Apply migration
	result, err := runner.ApplyMigration(req.SQL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, supabase.ErrorResponse{
			Error: supabase.ErrorDetail{
				Code:    "MIGRATION_FAILED",
				Message: "Failed to apply schema",
				Details: err.Error(),
			},
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

// DeleteProject handles DELETE /api/projects/:id
func (h *Handler) DeleteProject(c *gin.Context) {
	projectID := c.Param("id")

	// Get project to get reference
	project, err := h.storage.GetProject(projectID)
	if err != nil {
		c.JSON(http.StatusNotFound, supabase.ErrorResponse{
			Error: supabase.ErrorDetail{
				Code:    "PROJECT_NOT_FOUND",
				Message: "Project not found",
			},
		})
		return
	}

	// Delete from Supabase (optional - might want to keep for POC)
	deleteFromSupabase := c.Query("delete_remote") == "true"
	if deleteFromSupabase {
		if err := h.supabaseClient.DeleteProject(project.ProjectRef); err != nil {
			// Log but don't fail - we'll still delete locally
			fmt.Printf("Warning: Failed to delete project from Supabase: %v\n", err)
		}
	}

	// Delete from local storage
	if err := h.storage.DeleteProject(projectID); err != nil {
		c.JSON(http.StatusInternalServerError, supabase.ErrorResponse{
			Error: supabase.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to delete project",
				Details: err.Error(),
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Project deleted successfully",
		"id":      projectID,
	})
}

// GetStats handles GET /api/stats
func (h *Handler) GetStats(c *gin.Context) {
	stats, err := h.storage.GetStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, supabase.ErrorResponse{
			Error: supabase.ErrorDetail{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to get statistics",
				Details: err.Error(),
			},
		})
		return
	}

	c.JSON(http.StatusOK, stats)
}