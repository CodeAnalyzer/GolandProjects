package store

import "strings"

// Вспомогательные функции для NULL значений
func sanitizeUTF8String(value string) string {
	if value == "" {
		return value
	}
	return strings.ToValidUTF8(value, "")
}

func sanitizeNullableJSON(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return value
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil
	}
	return sanitizeUTF8String(text)
}

func NullableString(value string) interface{} {
	value = sanitizeUTF8String(value)
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func NullableInt(value int) interface{} {
	if value == 0 {
		return nil
	}
	return value
}

func NullableInt64(value int64) interface{} {
	if value == 0 {
		return nil
	}
	return value
}

func NullableProcID(procID int64) interface{} {
	if procID == 0 {
		return nil
	}
	return procID
}
