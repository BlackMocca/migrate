package elasticsearch

import (
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strings"

	"github.com/spf13/cast"
)

var (
	regexExludeHeader = regexp.MustCompile(`^([\w-]+:\w+)(,[\w-]+:\w+)*$`)
)

type HttpSeed struct {
	RestConfig *RestConfig
}

type RestConfig struct {
	MigrationPath string      `json:"-"`
	URL           string      `json:"url"`
	Method        string      `json:"method"`
	Path          string      `json:"path"`
	QueryParams   url.Values  `json:"query_params"`
	Header        http.Header `json:"header"`
	FilePath      string      `json:"file_path"`
	BodyType      string      `json:"body_type"`
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
		case "url":
			ptr.URL = cast.ToString(val)
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
		case "file_path":
			if val != nil {
				ptr.FilePath = cast.ToString(val)
			}
		case "body_type":
			if val != nil {
				ptr.BodyType = cast.ToString(val)
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

func (r *RestConfig) ExcludeHeader(excludeHeader string) error {
	//key1:val1,key2:val2
	if excludeHeader != "" {
		if !regexExludeHeader.MatchString(excludeHeader) {
			return errors.New("exclude_header incorrect format parameter")
		}
		if r.Header != nil {
			headers := strings.Split(excludeHeader, ",")
			for _, h := range headers {
				keyValue := strings.Split(h, ":")
				if len(keyValue) > 1 {
					key := keyValue[0]
					val := keyValue[1]
					r.Header.Add(key, val)
				}

			}

		}
	}

	return nil
}
