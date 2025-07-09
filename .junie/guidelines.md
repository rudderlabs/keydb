# Go Rules

## rudder-go-kit

https://github.com/rudderlabs/rudder-go-kit opinionated library for rudderstack. Should be used for all Golang projects,
for common things like logging, config, http, etc.

### JSON

Always use `jsonrs` over `encoding/json`:

```go
import "github.com/rudderlabs/rudder-go-kit/jsonrs"
```

### HTTP

```go
import "github.com/rudderlabs/rudder-go-kit/httputil"
```

**Request**: Always close response body properly:

DO NOT:
```go
defer resp.Body.Close()
```

DO:
```go
defer func() { httputil.CloseResponse(resp) }()
```
### Logging

```go
import (
    "github.com/rudderlabs/rudder-go-kit/logger"
    obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)
```

**Use non-sugared methods** with `n` suffix and structured fields:

DO NOT:
```go
fmt.Println("foo")
log.Info("starting", port, port)
log.Infow("starting", port, port)
log.Infof("starting port %d", port)
log.Infon("starting on port %d", logger.NewIntField("port", int64(port))) // String formatting in message
```

DO:
```go
log.Infon("starting", logger.NewIntField("port", int64(port)))
log.Errorn("operation failed", obskit.Error(err))
```

**Field naming**: Use camelCase, no uppercase acronyms:

DO NOT:
```go
logger.NewStringField("client_id", clientID)  // snake_case
logger.NewStringField("clientID", clientID)   // uppercase acronym
```

DO:
```go
logger.NewStringField("clientId", clientID)   // camelCase
```

### Config

```go
import "github.com/rudderlabs/rudder-go-kit/config"
```

**Init once** in main with service prefix:

DO NOT:
```go
// Global/singleton pattern
var globalConfig = config.New()
```

DO:
```go
// In main only
conf := kitconfig.New(kitconfig.WithEnvPrefix("SERVICE_NAME"))
```

**Always provide defaults**:

DO NOT:
```go
port := conf.GetInt("HTTP.Port") // No default
```

DO:
```go
port := conf.GetInt("HTTP.Port", 8080)
timeout := conf.GetDuration("HTTP.ShutdownTimeout", 10, time.Second)
```

**Testing**: Use `conf.Set()` only in tests:

DO NOT:
```go
// In production code
conf.Set("HTTP.Port", 8080)
```

DO:
```go
// In tests only
conf.Set("HTTP.Port", port)
```

### Stats

```go
import "github.com/rudderlabs/rudder-go-kit/stats"
```

**Init once** in main:
```go
stat := stats.NewStats(conf, logFactory, svcMetric.NewManager(), []stats.Option{
    stats.WithServiceName(serviceName),
    stats.WithServiceVersion(version),
})

if err := stat.Start(ctx, stats.DefaultGoRoutineFactory); err != nil {
    log.Errorn("Failed to start Stats", obskit.Error(err))
    return err
}
defer stat.Stop()
```

## PostgreSQL

Always use `sql.DB` with pgx driver:
```go
import (
    "database/sql"
    _ "github.com/jackc/pgx/v5/stdlib"
)
```

**Connection**:
```go
db, err := sql.Open("pgx", databaseURL)
if err := db.PingContext(ctx); err != nil {
    _ = db.Close()
    return nil, err
}
```

**Queries**: Always use context-aware methods:
```go
// Single row
err := db.QueryRowContext(ctx, "SELECT name FROM users WHERE id = $1", userID).Scan(&name)

// Multiple rows
rows, err := db.QueryContext(ctx, "SELECT id, name FROM users WHERE active = $1", true)
defer rows.Close()
```

**Monitoring**: Use `sqlutil.MonitorDatabase` in goroutine (blocking call):
```go
g.Go(func() error {
    sqlutil.MonitorDatabase(ctx, conf, stat, db, "service-name")
    return nil
})
```

**CRITICAL**: `sqlutil.MonitorDatabase` is a **blocking call** - it runs continuously until the context is cancelled. Always run it in a goroutine or errgroup.

**Important**: 
- Use unique identifiers for each database pool to avoid metric conflicts
- Run monitoring once per database pool

**Migrations**: Use golang-migrate with pgx v5 driver

## Context

Always pass context as first parameter:

DO NOT:
```go
func runWith(conf *kitconfig.Config, ctx context.Context) error
```

DO:
```go
func runWith(ctx context.Context, conf *kitconfig.Config) error
```

## Testing

### Package Naming

Use `_test` suffix to test public API only:

DO NOT:
```go
package mypackage // Tests internal implementation
```

DO:
```go
package mypackage_test // Tests public API only
```

### PostgreSQL Integration

```go
import (
    "github.com/ory/dockertest/v3"
    "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
)
```

```go
func TestDB(t *testing.T) {
    pool, err := dockertest.NewPool("")
    require.NoError(t, err)
    
    pgResource, err := postgres.Setup(pool, t)
    require.NoError(t, err)
    
    db := pgResource.DB
}
```

### Critical Testing Rules

**1. Always use `require` (not `assert`)**:

DO NOT:
```go
assert.Equal(t, expected, actual)  // Continues on failure
assert.NoError(t, err)             // Continues on failure
```

DO:
```go
require.Equal(t, expected, actual, "descriptive message")
require.NoError(t, err, "descriptive message")
```

**2. Use `t.Log()` instead of comments**:

DO NOT:
```go
// Create the token
token := createTestToken()
// Remove the token
err := repo.RemoveByCode(ctx, token.Code)
```

DO:
```go
t.Log("Creating test token")
token := createTestToken()
t.Log("Removing token by authorization code")
err := repo.RemoveByCode(ctx, token.Code)
```

**3. Interface compliance at compile time**:

DO NOT:
```go
// In test files
func TestMyAdapter(t *testing.T) {
    var _ SomeInterface = adapter
}
```

DO:
```go
// In source files
var _ SomeInterface = (*MyAdapter)(nil)
```

**4. Deep equality comparisons**:

DO NOT:
```go
require.Equal(t, original.ClientID, retrieved.ClientID)
require.Equal(t, original.Code, retrieved.Code)
require.Equal(t, original.CreatedAt, retrieved.CreatedAt)
```

DO:
```go
require.Equal(t, original, retrieved)
```

**5. Consistent timestamps**:

DO NOT:
```go
ClientIDIssuedAt: time.Now().Unix(),                              // First timestamp
"client_id_issued_at": ` + fmt.Sprintf("%d", time.Now().Unix()) // Second timestamp
```

DO:
```go
now := time.Now().Unix()
ClientIDIssuedAt: now,
"client_id_issued_at": ` + fmt.Sprintf("%d", now)
```

**6. Test isolation**:

**CRITICAL**: Ensure test isolation to prevent conflicts and side effects.

**Unique identifiers**:
```go
// BAD - Shared identifiers
_, err := authStore.Retrieve("non-existent-state") // Conflict!

// GOOD - Unique per test
testName := strings.ReplaceAll(t.Name(), "/", "-")
state := "test-state-" + testName
t.Logf("Using test state: %s", state)  // Always log identifiers
```

**Fresh dependencies**:
```go
// BAD - Shared dependencies
var globalConfig = config.New()

// GOOD - Fresh per test
func TestSomething(t *testing.T) {
    conf := config.New()  // Fresh config per test
    log := logger.NOP     // Fresh logger per test
}
```

**7. Async operations**:

DO NOT:
```go
time.Sleep(1 * time.Second) // Flaky timing
```

DO:
```go
require.Eventually(t, func() bool {
    return someCondition
}, 10*time.Second, 100*time.Millisecond, "timeout message")
```

### Test Structure (Arrange-Act-Assert)
```go
func TestSomething(t *testing.T) {
    // Arrange
    conf := config.New()
    
    // Act
    result, err := handler.DoSomething()
    
    // Assert
    require.NoError(t, err, "descriptive message")
    require.Equal(t, expected, result, "descriptive message")
}
```

## Error Handling

### Error Wrapping

When you cannot handle an error and need to propagate it, you SHOULD consider wrapping it using `fmt.Errorf("...: %w", err)` if additional context would make it easier for understanding the error.

**Avoid using "failure", "error", or similar words when wrapping. Instead focus on describing the operation that caused the issue.**

DO NOT:
```go
return fmt.Errorf("failed to connect: %w", err)
return fmt.Errorf("error connecting: %w", err)
return fmt.Errorf("failure in connection: %w", err)
```

DO:
```go
return fmt.Errorf("connecting to database: %w", err)
return fmt.Errorf("parsing config: %w", err)
return fmt.Errorf("starting server: %w", err)
```

### Modern Error Handling Patterns

**CRITICAL**: Use `errors.Is()` and `errors.As()` instead of direct comparisons/type assertions.

**Error comparison**:

DO NOT:
```go
if err == sql.ErrNoRows {
    return fmt.Errorf("user not found")
}
```

DO:
```go
if errors.Is(err, sql.ErrNoRows) {
    return model.ErrItemNotFound
}
```

**Error unwrapping**:

DO NOT:
```go
if oauthErr, ok := err.(*model.OAuthError); ok {
    return oauthErr.StatusCode
}
```

DO:
```go
var oauthErr *model.OAuthError
if errors.As(err, &oauthErr) {
    return oauthErr.StatusCode
}
```

### Domain Errors
```go
// Define in model package
var (
    ErrItemNotFound = errors.New("item not found")
    ErrUnauthorized = errors.New("unauthorized")
)
```

### Never Ignore Errors

DO NOT:
```go
result, _ := someFunction()
```

DO:
```go
result, err := someFunction()
if err != nil {
    return fmt.Errorf("operation: %w", err)
}
```

### Avoid Duplicate Logging

DO NOT:
```go
log.Errorn("Failed to connect", obskit.Error(err))
return fmt.Errorf("connecting: %w", err) // Duplicate logging
```

DO:
```go
// Either log and handle locally
log.Errorn("Failed to connect", obskit.Error(err))
return nil

// OR return without logging (let caller log)
return fmt.Errorf("connecting: %w", err)
```

### Error Wrapping

DO NOT:
```go
return fmt.Errorf("failed to start server: %w", err)
// Creates: "failed to start server: failed to init database: failed to connect..."
```

DO:
```go
return fmt.Errorf("starting server: %w", err)
// Creates: "starting server: initializing database: connecting to database..."
```

## Code Organization

### Constants

DO NOT:
```go
if authMethod == "client_secret_basic" {
    // Magic strings
}
```

DO:
```go
const (
    AuthMethodClientSecretBasic = "client_secret_basic"
    AuthMethodNone             = "none"
)

if authMethod == AuthMethodClientSecretBasic {
    // Use constants
}
```

### Interfaces

**Naming**: Use "-er" suffix when possible:
```go
type ClientStorer interface {
    Store(client *Client) error
    Get(clientID string) (*Client, error)
}
```

**Size**: Keep small and focused:

DO NOT:
```go
type MegaInterface interface {
    Store() error
    Get() error
    Delete() error
    Update() error
    Validate() error
    Transform() error
}
```

DO:
```go
type ClientReader interface {
    Get(clientID string) (*Client, error)
}

type ClientWriter interface {
    Store(client *Client) error
}
```

### Package Structure
```
internal/
  model/           # Data structures, DTOs
  repo/           # Data access layer  
  oauth/          # Business logic
```

## Naming Conventions

### Avoid Stuttering

**Critical**: Avoid repeating context that's already clear from package, type, or receiver names.

**Packages**: Use short, descriptive names:

DO NOT:
```go
package oauthclient
type OAuthClient struct {} // stuttering: oauth + OAuthClient

package httphandler  
func HTTPHandler() {} // stuttering: http + HTTPHandler
```

DO:
```go
package oauth
type Client struct {} // Clear from package context

package handler
func OAuth() {} // Clear from package context
```

**Structs and Methods**: Don't repeat type name in method names:

DO NOT:
```go
type Client struct {}
func (c *Client) GetClient() *Client     // stuttering: Client.GetClient
func (c *Client) StoreClient() error     // stuttering: Client.StoreClient
func (c *Client) DeleteClient() error    // stuttering: Client.DeleteClient

type AuthRequest struct {}
func (r *AuthRequest) ValidateAuthRequest() error // stuttering
```

DO:
```go
type Client struct {}
func (c *Client) Get() *Client      // Clear from receiver context
func (c *Client) Store() error      // Clear from receiver context  
func (c *Client) Delete() error     // Clear from receiver context

type AuthRequest struct {}
func (r *AuthRequest) Validate() error // Clear from receiver context
```

**Functions**: Don't repeat package name in function names:

DO NOT:
```go
package oauth
func OAuthValidate() error    // stuttering: oauth.OAuthValidate
func OAuthGenerate() string   // stuttering: oauth.OAuthGenerate

package repo
func RepoStore() error        // stuttering: repo.RepoStore
func RepoGet() error          // stuttering: repo.RepoGet
```

DO:
```go
package oauth
func Validate() error     // Clear: oauth.Validate
func Generate() string    // Clear: oauth.Generate

package repo  
func Store() error        // Clear: repo.Store
func Get() error          // Clear: repo.Get
```

**Variables**: Use context-appropriate names:

DO NOT:
```go
type Client struct {
    ClientID     string  // stuttering in struct
    ClientSecret string  // stuttering in struct
}

var oauthClient *oauth.Client // stuttering with package
```

DO:
```go
type Client struct {
    ID     string  // Clear from type context
    Secret string  // Clear from type context
}

var client *oauth.Client // Clear from package context
```

### Environment Variables

**Required vs Optional**:

DO NOT:
```go
databaseURL := conf.GetString("DATABASE_URL", "default_url") // Bad default
```

DO:
```go
// Optional with sensible default
port := conf.GetInt("HTTP.Port", 8080)

// Required - fail fast
databaseURL := conf.GetString("DATABASE_URL", "")
if databaseURL == "" {
    return fmt.Errorf("DATABASE_URL is required")
}
```

## Database Migrations

**Naming**: Use descriptive names with timestamps:
```
20250701152400_create_oauth_clients.up.sql
20250701152400_create_oauth_clients.down.sql
```

**Fields**: snake_case in DB, camelCase in Go:
```sql
CREATE TABLE oauth_clients (
    client_id VARCHAR(255) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

```go
type Client struct {
    ClientID  string    `json:"client_id"`
    CreatedAt time.Time `json:"created_at"`
}
```

## Security

**Never log secrets**:

DO NOT:
```go
log.Infon("client registered", 
    logger.NewStringField("clientId", client.ClientID),
    logger.NewStringField("clientSecret", client.ClientSecret)) // BAD!
```

DO:
```go
log.Infon("client registered", 
    logger.NewStringField("clientId", client.ClientID)) // Only non-sensitive
```

## Program Lifecycle

**Never use `os.Exit` directly**:

DO NOT:
```go
os.Exit(1)           // BAD! Prevents defers from running
log.Fatal("error")   // BAD! Calls os.Exit internally
log.Fatalf("error")  // BAD! Calls os.Exit internally
log.Fatalln("error") // BAD! Calls os.Exit internally
```

DO:
```go
func main() {
    os.Exit(run()) // Only call os.Exit from main()
}

func run() int {
    defer cleanup() // This will run properly
    if err := doWork(); err != nil {
        return 1
    }
    return 0
}
```

## Development Workflow

### Validation
Run `make lint && make test` after major changes 

### Conventional Commits
Format: `<type>[scope]: <description>`

**Types**: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

**Examples**:
```bash
git commit -m "feat(auth): add OAuth 2.0 client registration endpoint"
git commit -m "fix(db): resolve connection pool leak in token storage"
git commit -m "refactor(repo): simplify error handling in auth requests"
```

**Scopes**: `auth`, `db`, `api`, `repo`, `model`, `config`, `test`, `build`
