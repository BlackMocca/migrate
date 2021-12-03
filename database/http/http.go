package elasticsearch

import (
	"net/http"
	"net/url"
	"reflect"

	"github.com/spf13/cast"
)

type HttpSeed struct {
	RestConfig *RestConfig
}

type RestConfig struct {
	MigrationPath string      `json:"-"`
	Method        string      `json:"method"`
	Path          string      `json:"path"`
	QueryParams   url.Values  `json:"query_params"`
	Header        http.Header `json:"header"`
	Body          interface{} `json:"body"`
}

func (r RestConfig) IsZero() bool {
	if r.Method == "" || r.Path == "" {
		return true
	}

	return false
}

func NewRestConfig(params map[string]interface{}) *RestConfig {
	ptr := new(RestConfig)
	queryParams := url.Values{}
	header := map[string][]string{}
	for key, val := range params {
		switch key {
		case "method":
			ptr.Method = cast.ToString(val)
		case "path":
			ptr.Path = cast.ToString(val)
		case "query_params":
			if val != nil {
				for qKey, qVals := range val.(map[string]interface{}) {
					if reflect.TypeOf(qVals).Kind() == reflect.String {
						queryParams.Add(cast.ToString(qKey), cast.ToString(qVals))
					} else if reflect.TypeOf(qVals).Kind() == reflect.Slice {
						for _, elem := range qVals.([]interface{}) {
							queryParams.Add(cast.ToString(qKey), cast.ToString(elem))
						}
					}
				}
			}
		case "header":
			if val != nil {
				for hKey, hVals := range val.(map[string]interface{}) {
					if _, ok := header[hKey]; !ok {
						header[hKey] = make([]string, 0)
					}
					header[hKey] = append(header[hKey], cast.ToString(hVals))
				}
			}
		case "body":
			if val != nil {
				ptr.Body = val
			}
		}
	}
	ptr.QueryParams = queryParams
	ptr.Header = header

	return ptr
}
