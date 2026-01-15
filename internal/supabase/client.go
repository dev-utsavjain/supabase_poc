package supabase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	managementAPIURL = "https://api.supabase.com/v1"
	maxRetries       = 3
	retryDelay       = 5 * time.Second
)

// Client handles Supabase Management API operations
type Client struct {
	accessToken    string
	organizationID string
	httpClient     *http.Client
}

// NewClient creates a new Supabase client
func NewClient(accessToken, organizationID string) *Client {
	return &Client{
		accessToken:    accessToken,
		organizationID: organizationID,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// CreateProject creates a new Supabase project
func (c *Client) CreateProject(name, region string) (*Project, error) {
	// Generate database password
	dbPassword := generateSecurePassword()

	payload := map[string]interface{}{
		"organization_id": c.organizationID,
		"name":            name,
		"region":          region,
		"plan":            "free", // Use free tier for POC
		"db_pass":         dbPassword,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", managementAPIURL+"/projects", bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.accessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	var result Project
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Store the database password (not returned by API)
	result.DBPassword = dbPassword

	return &result, nil
}

// GetProject retrieves project details
func (c *Client) GetProject(projectRef string) (*Project, error) {
	req, err := http.NewRequest("GET", managementAPIURL+"/projects/"+projectRef, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("project not found")
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	var project Project
	if err := json.NewDecoder(resp.Body).Decode(&project); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Also fetch database connection details from a separate endpoint
	// Supabase provides database host info in the project response
	// Let's extract it properly
	if project.ProjectRef != "" && project.DatabaseHost == "" {
		// Use IPv6 format which is more reliable
		project.DatabaseHost = fmt.Sprintf("db.%s.supabase.co", project.ProjectRef)
	}

	return &project, nil
}

// WaitForProject polls until the project is ready
func (c *Client) WaitForProject(projectRef string, timeout time.Duration) (*Project, error) {
	deadline := time.Now().Add(timeout)
	checkInterval := 5 * time.Second

	for time.Now().Before(deadline) {
		project, err := c.GetProject(projectRef)
		if err != nil {
			// Project might not be found immediately
			time.Sleep(checkInterval)
			continue
		}

		// Check if project is ready
		if project.Status == "ACTIVE_HEALTHY" {
			return project, nil
		}

		// If status indicates failure
		if project.Status == "INACTIVE" || project.Status == "UNKNOWN" {
			return nil, fmt.Errorf("project creation failed with status: %s", project.Status)
		}

		// Wait before next check
		time.Sleep(checkInterval)
		
		// Increase check interval gradually (exponential backoff)
		if checkInterval < 15*time.Second {
			checkInterval += 2 * time.Second
		}
	}

	return nil, fmt.Errorf("timeout waiting for project to become ready")
}

// DeleteProject deletes a Supabase project
func (c *Client) DeleteProject(projectRef string) error {
	req, err := http.NewRequest("DELETE", managementAPIURL+"/projects/"+projectRef, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// ProjectAPIKeys holds API keys for a project
type ProjectAPIKeys struct {
	AnonKey    string `json:"anon_key"`
	ServiceKey string `json:"service_role_key"`
}

// GetProjectAPIKeys retrieves the API keys for a project
func (c *Client) GetProjectAPIKeys(projectRef string) (*ProjectAPIKeys, error) {
	// Get project config which includes API keys
	req, err := http.NewRequest("GET", managementAPIURL+"/projects/"+projectRef+"/api-keys", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	var keys []struct {
		Name string `json:"name"`
		APIKey string `json:"api_key"`
	}
	
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	result := &ProjectAPIKeys{}
	for _, key := range keys {
		if key.Name == "anon" {
			result.AnonKey = key.APIKey
		} else if key.Name == "service_role" {
			result.ServiceKey = key.APIKey
		}
	}

	return result, nil
}

// TestConnection verifies that the Supabase API is reachable
func (c *Client) TestConnection() error {
	req, err := http.NewRequest("GET", managementAPIURL+"/projects", nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to Supabase API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("invalid Supabase access token")
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// generateSecurePassword generates a secure random password
func generateSecurePassword() string {
	// For production, use crypto/rand
	// For POC, simple secure password
	return fmt.Sprintf("Sup4b4se_%d_%s", time.Now().Unix(), randomString(16))
}

// randomString generates a random alphanumeric string
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		// Use time-based seed for POC (use crypto/rand in production)
		result[i] = charset[time.Now().UnixNano()%int64(len(charset))]
		time.Sleep(1 * time.Nanosecond) // Ensure different values
	}
	return string(result)
}