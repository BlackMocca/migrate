package elasticsearch

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
)

const (
	INDEX_TEMPLATE = "${index}"
	BODY_TYPE_BULK = "bulk"
)

type Elasticsearch struct {
	Index      string
	RestConfig *RestConfig
}

type RestConfig struct {
	MigrationPath string            `json:"-"`
	Method        string            `json:"method"`
	Path          string            `json:"path"`
	Header        map[string]string `json:"header"`
	Body          interface{}       `json:"body"`
	BodyType      string            `json:"body_type"`
	BodyPathFile  string            `json:"body_path_file"`
}

func (r RestConfig) IsZero() bool {
	if r.Method == "" || r.Path == "" {
		return true
	}

	return false
}

func (r RestConfig) ToHTTPHeader() http.Header {
	h := http.Header{}
	h.Add("Content-Type", "application/json; charset=UTF-8")
	if r.Header == nil || len(r.Header) == 0 {
		return h
	}

	for key, val := range r.Header {
		h.Set(key, val)
	}

	return h
}

func (r *RestConfig) ReplaceStringWithIndex(index string) {
	r.Path = strings.ReplaceAll(r.Path, INDEX_TEMPLATE, index)
	if r.Header != nil {
		for key, val := range r.Header {
			r.Header[key] = strings.ReplaceAll(val, INDEX_TEMPLATE, index)
		}
	}

	if r.Body != nil {
		switch r.BodyType {
		case BODY_TYPE_BULK:
			pathFile := fmt.Sprintf("%s/%s", strings.Trim(r.MigrationPath, "/"), strings.Trim(r.BodyPathFile, "/"))
			file, err := os.Open(pathFile)
			if err != nil {
				panic(err)
			}
			defer file.Close()

			var strs = []string{}
			scanner := bufio.NewScanner(file)
			// optionally, resize scanner's capacity for lines over 64K, see next example
			for scanner.Scan() {
				lineTxt := scanner.Text()
				if lineTxt != "" && lineTxt != "\n" {
					strs = append(strs, lineTxt)
				}
			}

			if err := scanner.Err(); err != nil {
				panic(err)
			}

			r.Body = strs
		default:
			bu, err := json.Marshal(r.Body)
			if err != nil {
				panic(err)
			}

			str := strings.ReplaceAll(string(bu), INDEX_TEMPLATE, index)

			r.Body = str
		}
	}
}
