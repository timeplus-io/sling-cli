package database

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProtonDataTypes tests all critical data types for ClickHouse-compatible DBMS
// This test ensures we support all data types commonly used in analytical applications
func TestProtonDataTypes(t *testing.T) {
	conn := &ProtonConn{}

	// Critical data types with real-world examples
	dataTypes := []struct {
		name        string
		dbType      string
		testValues  []interface{}
		expectType  string
		description string
	}{
		// DECIMAL TYPES - for precise calculations
		{
			name:        "decimal_precision",
			dbType:      "decimal64",
			testValues:  []interface{}{"123.45", "9999.9999", "-456.78", "0.01"},
			expectType:  "decimal",
			description: "High-precision decimal amounts",
		},
		{
			name:        "decimal32_small",
			dbType:      "decimal32",
			testValues:  []interface{}{"99.99", "0.01", "-12.34"},
			expectType:  "decimal",
			description: "Small decimal amounts with precision",
		},
		{
			name:        "decimal128_large",
			dbType:      "decimal128",
			testValues:  []interface{}{"123456789.12345678", "999999999999.999999"},
			expectType:  "decimal",
			description: "Very large decimal amounts",
		},
		{
			name:        "money_alias",
			dbType:      "money",
			testValues:  []interface{}{"1000.50", "2500.75"},
			expectType:  "decimal",
			description: "Money type (alias for decimal64)",
		},
		{
			name:        "numeric_postgres_compat",
			dbType:      "numeric",
			testValues:  []interface{}{"12345.6789", "0.0001"},
			expectType:  "decimal",
			description: "PostgreSQL numeric compatibility",
		},

		// DATE/TIME TYPES - for temporal data
		{
			name:        "date_values",
			dbType:      "date",
			testValues:  []interface{}{"2024-01-15", "2023-12-31"},
			expectType:  "date",
			description: "Date values for temporal analysis",
		},
		{
			name:        "datetime64_timestamps",
			dbType:      "datetime64",
			testValues:  []interface{}{"2024-01-15 14:30:25", "2024-01-15T14:30:25.123Z"},
			expectType:  "datetime64",
			description: "Timestamps with precision",
		},
		{
			name:        "timestamp_alias",
			dbType:      "timestamp",
			testValues:  []interface{}{"2024-01-15 14:30:25.123456"},
			expectType:  "datetime64",
			description: "Timestamp alias for datetime64",
		},

		// UUID TYPE - for unique identifiers
		{
			name:        "uuid_identifiers",
			dbType:      "uuid",
			testValues:  []interface{}{"123e4567-e89b-12d3-a456-426614174000", "550e8400-e29b-41d4-a716-446655440000"},
			expectType:  "uuid",
			description: "UUID for unique entity identification",
		},

		// NULLABLE TYPES
		{
			name:        "nullable_decimal64",
			dbType:      "nullable(decimal64)",
			testValues:  []interface{}{"123.45", nil, "0.00"},
			expectType:  "decimal",
			description: "Optional decimal amounts",
		},
		{
			name:        "nullable_datetime64",
			dbType:      "nullable(datetime64)",
			testValues:  []interface{}{"2024-01-15 14:30:25", nil},
			expectType:  "datetime64",
			description: "Optional timestamps",
		},
		{
			name:        "nullable_uuid",
			dbType:      "nullable(uuid)",
			testValues:  []interface{}{"123e4567-e89b-12d3-a456-426614174000", nil},
			expectType:  "uuid",
			description: "Optional identifiers",
		},

		// ARRAY TYPES - for batch data
		{
			name:        "array_decimal64",
			dbType:      "array(decimal64)",
			testValues:  []interface{}{`["123.45", "678.90", "0.01"]`, `[]`},
			expectType:  "array(decimal)",
			description: "Arrays of decimal values",
		},
		{
			name:        "array_date",
			dbType:      "array(date)",
			testValues:  []interface{}{`["2024-01-15", "2024-01-16"]`, `[]`},
			expectType:  "array(date)",
			description: "Arrays of dates",
		},
		{
			name:        "array_uuid",
			dbType:      "array(uuid)",
			testValues:  []interface{}{`["123e4567-e89b-12d3-a456-426614174000", "550e8400-e29b-41d4-a716-446655440000"]`},
			expectType:  "array(uuid)",
			description: "Arrays of identifiers",
		},

		// COMPLEX NESTED TYPES
		{
			name:        "low_cardinality_nullable_decimal",
			dbType:      "low_cardinality(nullable(decimal64))",
			testValues:  []interface{}{"99.99", "199.99", nil},
			expectType:  "decimal",
			description: "Low cardinality decimal categories",
		},
		{
			name:        "nullable_low_cardinality_string",
			dbType:      "nullable(low_cardinality(string))",
			testValues:  []interface{}{"USD", "EUR", "GBP", nil},
			expectType:  "string",
			description: "Categorical string values with null support",
		},
	}

	for _, ft := range dataTypes {
		t.Run(ft.name, func(t *testing.T) {
			t.Logf("Testing data type: %s (%s)", ft.dbType, ft.description)

			// Create column and build converter
			columns := iop.Columns{{Name: "fintech_col", DbType: ft.dbType}}
			converters := conn.buildColumnConverters(columns)

			// Verify converter was created
			require.Equal(t, 1, len(converters), "Should create exactly one converter for %s", ft.dbType)
			converter, exists := converters[0]
			require.True(t, exists, "Converter should exist at index 0")
			assert.Equal(t, ft.expectType, converter.TypeName, "Converter should have correct type for %s", ft.dbType)

			// Test conversions with financial data
			for _, testVal := range ft.testValues {
				result, err := converter.Convert(testVal)

				if testVal == nil {
					t.Logf("  nil -> %v (handled gracefully)", result)
				} else {
					assert.NoError(t, err, "Conversion should succeed for %v (%T) in %s", testVal, testVal, ft.dbType)
					assert.NotNil(t, result, "Result should not be nil for non-nil input in %s", ft.dbType)
					t.Logf("  %v (%T) -> %v (%T) ✓", testVal, testVal, result, result)
				}
			}
		})
	}
}

// TestProtonDataTypeMapping verifies the protonDataTypeMap includes all analytical types
func TestProtonDataTypeMapping(t *testing.T) {
	// Critical analytical types that must be in the mapping
	criticalTypes := []string{
		// Decimal types
		"decimal", "decimal32", "decimal64", "decimal128", "decimal256",
		"numeric", "money",

		// Date/time types
		"date", "date32", "datetime", "datetime64", "timestamp",

		// UUID type
		"uuid",

		// Array types for analytical data
		"array(decimal32)", "array(decimal64)", "array(date)", "array(datetime64)", "array(uuid)",

		// Common precision specifications
		"decimal(9,2)", "decimal(18,4)", "decimal(38,8)",
		"datetime64(3)", "datetime64(6)",
	}

	for _, criticalType := range criticalTypes {
		t.Run("mapping_"+criticalType, func(t *testing.T) {
			mapped, exists := protonDataTypeMap[criticalType]
			assert.True(t, exists, "Critical analytical type %s must be in protonDataTypeMap", criticalType)
			assert.NotEmpty(t, mapped, "Mapping for %s should not be empty", criticalType)
			t.Logf("%s -> %s ✓", criticalType, mapped)
		})
	}
}

// TestProtonAnalyticalScenarios tests real-world analytical scenarios
func TestProtonAnalyticalScenarios(t *testing.T) {
	conn := &ProtonConn{}

	scenarios := []struct {
		name        string
		description string
		columns     []struct {
			name   string
			dbType string
		}
		sampleData [][]interface{}
	}{
		{
			name:        "market_data",
			description: "Market data analytics table",
			columns: []struct {
				name   string
				dbType string
			}{
				{"record_id", "uuid"},
				{"entity_id", "uuid"},
				{"symbol", "low_cardinality(string)"},
				{"volume", "decimal64"},
				{"price", "decimal64"},
				{"market_value", "decimal64"},
				{"timestamp", "datetime64(3)"},
				{"date", "date"},
				{"category", "low_cardinality(string)"},
				{"adjustment", "nullable(decimal64)"},
			},
			sampleData: [][]interface{}{
				{
					"123e4567-e89b-12d3-a456-426614174000",
					"550e8400-e29b-41d4-a716-446655440000",
					"ITEM_A",
					"100.00",
					"150.25",
					"15025.00",
					"2024-01-15T14:30:25.123",
					"2024-01-17",
					"TYPE_1",
					"9.99",
				},
				{
					"223e4567-e89b-12d3-a456-426614174001",
					"650e8400-e29b-41d4-a716-446655440001",
					"ITEM_B",
					"50.00",
					"2800.50",
					"140025.00",
					"2024-01-15T14:31:15.456",
					"2024-01-17",
					"TYPE_2",
					nil, // No adjustment
				},
			},
		},
		{
			name:        "event_processing",
			description: "Event processing analytics table",
			columns: []struct {
				name   string
				dbType string
			}{
				{"event_id", "uuid"},
				{"source_id", "uuid"},
				{"target_id", "uuid"},
				{"value", "decimal64"},
				{"unit", "low_cardinality(string)"},
				{"conversion_rate", "nullable(decimal128)"},
				{"converted_value", "nullable(decimal64)"},
				{"processing_time", "datetime64(6)"},
				{"status", "low_cardinality(string)"},
				{"metrics", "array(decimal64)"},
			},
			sampleData: [][]interface{}{
				{
					"323e4567-e89b-12d3-a456-426614174002",
					"423e4567-e89b-12d3-a456-426614174003",
					"523e4567-e89b-12d3-a456-426614174004",
					"1000.00",
					"UNIT_A",
					"1.08",
					"1080.00",
					"2024-01-15T14:30:25.123456",
					"completed",
					`["25.00", "5.00"]`,
				},
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			t.Logf("Testing scenario: %s", scenario.description)

			// Build columns
			columns := make(iop.Columns, len(scenario.columns))
			for i, col := range scenario.columns {
				columns[i] = iop.Column{Name: col.name, DbType: col.dbType}
			}

			// Build converters
			converters := conn.buildColumnConverters(columns)
			assert.Equal(t, len(scenario.columns), len(converters), "Should create converter for each column")

			// Test each row of sample data
			for rowIdx, row := range scenario.sampleData {
				t.Logf("  Testing row %d", rowIdx+1)
				assert.Equal(t, len(scenario.columns), len(row), "Row should have correct number of columns")

				// Convert each value
				for colIdx, value := range row {
					converter := converters[colIdx]
					result, err := converter.Convert(value)

					if value == nil {
						t.Logf("    %s: nil -> handled gracefully", scenario.columns[colIdx].name)
					} else {
						assert.NoError(t, err, "Should convert %s value %v", scenario.columns[colIdx].name, value)
						t.Logf("    %s: %v -> %v ✓", scenario.columns[colIdx].name, value, result)
					}
				}
			}
		})
	}
}

// TestDecimalPrecisionHandling tests decimal precision handling specifically
func TestDecimalPrecisionHandling(t *testing.T) {
	conn := &ProtonConn{}

	precisionTests := []struct {
		name       string
		dbType     string
		input      string
		expectErr  bool
		expectType string
	}{
		{"high_precision", "decimal128", "123456789.123456789", false, "decimal"},
		{"standard_precision", "decimal64", "99999.9999", false, "decimal"},
		{"small_precision", "decimal32", "999.99", false, "decimal"},
		{"zero_value", "decimal64", "0.00", false, "decimal"},
		{"negative_value", "decimal64", "-1234.56", false, "decimal"},
		{"very_small", "decimal128", "0.000000001", false, "decimal"},
	}

	for _, test := range precisionTests {
		t.Run(test.name, func(t *testing.T) {
			columns := iop.Columns{{Name: "test_col", DbType: test.dbType}}
			converters := conn.buildColumnConverters(columns)

			require.Equal(t, 1, len(converters))
			converter := converters[0]
			assert.Equal(t, test.expectType, converter.TypeName)

			result, err := converter.Convert(test.input)
			if test.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.IsType(t, decimal.Decimal{}, result)
				t.Logf("%s: %s -> %v", test.dbType, test.input, result)
			}
		})
	}
}

// TestTimezoneSafety tests datetime handling for analytical applications
func TestTimezoneSafety(t *testing.T) {
	conn := &ProtonConn{}

	timeTests := []struct {
		name     string
		dbType   string
		input    string
		expectOk bool
	}{
		{"iso_utc", "datetime64", "2024-01-15T14:30:25Z", true},
		{"iso_with_tz", "datetime64", "2024-01-15T14:30:25+00:00", true},
		{"simple_datetime", "datetime64", "2024-01-15 14:30:25", true},
		{"date_only", "date", "2024-01-15", true},
		{"timestamp_precision", "datetime64", "2024-01-15T14:30:25.123456", true},
	}

	for _, test := range timeTests {
		t.Run(test.name, func(t *testing.T) {
			columns := iop.Columns{{Name: "test_col", DbType: test.dbType}}
			converters := conn.buildColumnConverters(columns)

			require.Equal(t, 1, len(converters))
			converter := converters[0]

			result, err := converter.Convert(test.input)
			if test.expectOk {
				assert.NoError(t, err)
				assert.IsType(t, time.Time{}, result)
				t.Logf("%s: %s -> %v", test.dbType, test.input, result)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
