//go:build go1.18
// +build go1.18

package azuread

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"
)

// DriverName is the name used to register the driver
const DriverName = "azuresql"

func init() {
	sql.Register(DriverName, &Driver{})
}

// Driver wraps the underlying MSSQL driver, but configures the Azure AD token provider
type Driver struct {
}

// Open returns a new connection to the database.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	c, _, err := NewConnector(dsn)
	if err != nil {
		return nil, err
	}

	return c.Connect(context.Background())
}

// NewConnector creates a new connector from a DSN.
// The returned connector may be used with sql.OpenDB.
func NewConnector(dsn string) (*mssql.Connector, AzureTokenCredentialFactory, error) {

	config, err := parse(dsn)
	if err != nil {
		return nil, nil, err
	}
	conn, err := newConnectorConfig(config)
	return conn, config, err
}

func NewConnectorConfig(params msdsn.Config) (*mssql.Connector, AzureTokenCredentialFactory, error) {
	config, err := parseConfig(params)
	if err != nil {
		return nil, nil, err
	}
	conn, err := newConnectorConfig(config)
	return conn, config, err
}

// newConnectorConfig creates a Connector from config.
func newConnectorConfig(config *azureFedAuthConfig) (*mssql.Connector, error) {
	switch config.fedAuthLibrary {
	case mssql.FedAuthLibraryADAL:
		return mssql.NewActiveDirectoryTokenConnector(
			config.mssqlConfig, config.adalWorkflow,
			func(ctx context.Context, serverSPN, stsURL string) (string, error) {
				return config.provideActiveDirectoryToken(ctx, serverSPN, stsURL)
			},
		)
	case mssql.FedAuthLibrarySecurityToken:
		return mssql.NewSecurityTokenConnector(
			config.mssqlConfig,
			func(ctx context.Context) (string, error) {
				return config.password, nil
			},
		)
	default:
		return mssql.NewConnectorConfig(config.mssqlConfig), nil
	}
}

type AzureTokenCredentialFactory interface {
	GetAzureCred(ctx context.Context, stsURL string) (azcore.TokenCredential, error)
}
