package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

func TestRedactInPlace(t *testing.T) {
	t.Run("redact the secret", func(t *testing.T) {
		// test most cases
		redactTestCase := &v1alpha.RedactTestCase{
			RootSafeValue:           "safe_value",
			RootSensitiveValue:      "sensitive_value",
			RepeatedSensitiveValues: []string{"sensitive_value_1", "sensitive_value_2"},
			RootEmbeddedObject: &v1alpha.RedactTestCase_EmbeddedObject{
				SafeString:      "safe_embedded",
				SensitiveString: "sensitive_embedded",
			},
			RepeatedEmbeddedObject: []*v1alpha.RedactTestCase_EmbeddedObject{
				{
					SafeString:      "safe_embedded_1",
					SensitiveString: "sensitive_embedded_1",
				},
				{
					SafeString:      "safe_embedded_2",
					SensitiveString: "sensitive_embedded_2",
				},
			},
			OneOfTest: &v1alpha.RedactTestCase_OneOfSensitiveValue{
				OneOfSensitiveValue: "one_of_sensitive_value",
			},
			MapSensitiveValues: map[string]string{
				"key_one": "sensitive_value_one",
				"key_two": "sensitive_value_two",
			},
			MapEmbeddedObjects: map[string]*v1alpha.RedactTestCase_EmbeddedObject{
				"embed_one": {
					SafeString:      "map_safe_embedded_1",
					SensitiveString: "map_sensitive_embedded_1",
				},
				"embed_two": {
					SafeString:      "map_safe_embedded_2",
					SensitiveString: "map_sensitive_embedded_2",
				},
			},
		}

		RedactInPlace(redactTestCase)

		assert.Equal(t, redactTestCase.RootSafeValue, "safe_value")
		assert.Equal(t, redactTestCase.RootSensitiveValue, redacted_string)
		assert.Equal(t, redactTestCase.RepeatedSensitiveValues[0], redacted_string)
		assert.Equal(t, redactTestCase.RepeatedEmbeddedObject[1].SafeString, "safe_embedded_2")
		assert.Equal(t, redactTestCase.RepeatedEmbeddedObject[1].SensitiveString, redacted_string)
		assert.Equal(t, redactTestCase.GetOneOfSensitiveValue(), redacted_string)
		assert.Equal(t, redactTestCase.MapSensitiveValues, map[string]string(nil))
		assert.Equal(t, redactTestCase.MapEmbeddedObjects["embed_two"].SafeString, "map_safe_embedded_2")
		assert.Equal(t, redactTestCase.MapEmbeddedObjects["embed_two"].SensitiveString, redacted_string)

		//test the one remainging one-of
		redactTestCase = &v1alpha.RedactTestCase{
			OneOfTest: &v1alpha.RedactTestCase_OneOfEmbeddedValue{
				OneOfEmbeddedValue: &v1alpha.RedactTestCase_EmbeddedObject{
					SafeString:      "safe_embedded_3",
					SensitiveString: "sensitive_embedded_3",
				},
			},
		}

		RedactInPlace(redactTestCase)

		assert.Equal(t, redactTestCase.GetOneOfEmbeddedValue().SafeString, "safe_embedded_3")
		assert.Equal(t, redactTestCase.GetOneOfEmbeddedValue().SensitiveString, redacted_string)
	})
}
