package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/elasticsearch"
	_ "github.com/golang-migrate/migrate/v4/database/elasticsearch"
	httpModels "github.com/golang-migrate/migrate/v4/database/http"
	_ "github.com/golang-migrate/migrate/v4/database/stub" // TODO remove again
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var (
	errInvalidSequenceWidth     = errors.New("Digits must be positive")
	errIncompatibleSeqAndFormat = errors.New("The seq and format options are mutually exclusive")
	errInvalidTimeFormat        = errors.New("Time format may not be empty")
)

func nextSeqVersion(matches []string, seqDigits int) (string, error) {
	if seqDigits <= 0 {
		return "", errInvalidSequenceWidth
	}

	nextSeq := uint64(1)

	if len(matches) > 0 {
		filename := matches[len(matches)-1]
		matchSeqStr := filepath.Base(filename)
		idx := strings.Index(matchSeqStr, "_")

		if idx < 1 { // Using 1 instead of 0 since there should be at least 1 digit
			return "", fmt.Errorf("Malformed migration filename: %s", filename)
		}

		var err error
		matchSeqStr = matchSeqStr[0:idx]
		nextSeq, err = strconv.ParseUint(matchSeqStr, 10, 64)

		if err != nil {
			return "", err
		}

		nextSeq++
	}

	version := fmt.Sprintf("%0[2]*[1]d", nextSeq, seqDigits)

	if len(version) > seqDigits {
		return "", fmt.Errorf("Next sequence number %s too large. At most %d digits are allowed", version, seqDigits)
	}

	return version, nil
}

func timeVersion(startTime time.Time, format string) (version string, err error) {
	switch format {
	case "":
		err = errInvalidTimeFormat
	case "unix":
		version = strconv.FormatInt(startTime.Unix(), 10)
	case "unixNano":
		version = strconv.FormatInt(startTime.UnixNano(), 10)
	default:
		version = startTime.Format(format)
	}

	return
}

// createCmd (meant to be called via a CLI command) creates a new migration
func createCmd(dir string, startTime time.Time, format string, name string, ext string, seq bool, seqDigits int, print bool) error {
	if seq && format != defaultTimeFormat {
		return errIncompatibleSeqAndFormat
	}

	var version string
	var err error

	dir = filepath.Clean(dir)
	ext = "." + strings.TrimPrefix(ext, ".")

	if seq {
		matches, err := filepath.Glob(filepath.Join(dir, "*"+ext))

		if err != nil {
			return err
		}

		version, err = nextSeqVersion(matches, seqDigits)

		if err != nil {
			return err
		}
	} else {
		version, err = timeVersion(startTime, format)

		if err != nil {
			return err
		}
	}

	versionGlob := filepath.Join(dir, version+"_*"+ext)
	matches, err := filepath.Glob(versionGlob)

	if err != nil {
		return err
	}

	if len(matches) > 0 {
		return fmt.Errorf("duplicate migration version: %s", version)
	}

	if err = os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	for _, direction := range []string{"up", "down"} {
		basename := fmt.Sprintf("%s_%s.%s%s", version, name, direction, ext)
		filename := filepath.Join(dir, basename)

		if err = createFile(filename); err != nil {
			return err
		}

		if print {
			absPath, _ := filepath.Abs(filename)
			log.Println(absPath)
		}
	}

	return nil
}

func createFile(filename string) error {
	// create exclusive (fails if file already exists)
	// os.Create() specifies 0666 as the FileMode, so we're doing the same
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)

	if err != nil {
		return err
	}

	return f.Close()
}

func gotoCmd(m *migrate.Migrate, v uint) error {
	if err := m.Migrate(v); err != nil {
		if err != migrate.ErrNoChange {
			return err
		}
		log.Println(err)
	}
	return nil
}

func upCmd(m *migrate.Migrate, limit int) error {
	if limit >= 0 {
		if err := m.Steps(limit); err != nil {
			if err != migrate.ErrNoChange {
				return err
			}
			log.Println(err)
		}
	} else {
		if err := m.Up(); err != nil {
			if err != migrate.ErrNoChange {
				return err
			}
			log.Println(err)
		}
	}
	return nil
}

func seedUpCmd(m *migrate.Migrate, limit int) error {
	if limit >= 0 {
		if err := m.Steps(limit); err != nil {
			if err != migrate.ErrNoChange {
				return err
			}
			log.Println(err)
		}
	} else {
		if err := m.SeedUp(); err != nil {
			if err != migrate.ErrNoChange {
				return err
			}
			log.Println(err)
		}
	}
	return nil
}

func seedDownCmd(m *migrate.Migrate, limit int) error {
	if limit >= 0 {
		if err := m.Steps(-limit); err != nil {
			if err != migrate.ErrNoChange {
				return err
			}
			log.Println(err)
		}
	} else {
		if err := m.SeedDown(); err != nil {
			if err != migrate.ErrNoChange {
				return err
			}
			log.Println(err)
		}
	}
	return nil
}

func seedUpInfluxCmd(database string, path string, token string) error {
	filesInfo, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	if len(filesInfo) == 0 {
		return migrate.ErrNoChange
	}

	for _, fInfo := range filesInfo {
		if !fInfo.IsDir() && strings.Contains(fInfo.Name(), ".up.txt") {
			filepath := path + "/" + fInfo.Name()

			txt, err := ioutil.ReadFile(filepath)
			if err != nil {
				return err
			}
			splitStr := strings.Split(string(txt), "\n")
			for index, data := range splitStr {
				data = strings.TrimSuffix(data, ",")
				curl := fmt.Sprintf(`curl -i -XPOST "%s" --header 'Authorization: Token %s' --data-raw "%s"`, database, path, data)
				resp, err := resty.New().SetDebug(false).R().
					SetHeader("Authorization", fmt.Sprintf("Token %s", token)).
					SetBody(data).
					Post(database)
				if err != nil {
					return fmt.Errorf(`
						Error of file: %s
						Line: %d
						Curl: %s
					`,
						fInfo.Name(),
						index,
						curl,
					)
				}
				if resp.StatusCode() == http.StatusNoContent || resp.StatusCode() == http.StatusOK {
					fmt.Println("sending data success: " + data)
					continue
				}
				log.Println(fmt.Sprintf("Error Line : %d", index))
				return fmt.Errorf(string(resp.Body()))
			}

			fmt.Println("migrate file: " + fInfo.Name() + " success")
		}
	}

	return nil
}

func seedUpElasticCmd(database string, path string, excludeHeader string, index string, skippError bool, debug bool) error {
	filesInfo, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	if len(filesInfo) == 0 {
		return migrate.ErrNoChange
	}

	for _, fInfo := range filesInfo {
		if !fInfo.IsDir() && strings.Contains(fInfo.Name(), ".up.json") {
			filepath := path + "/" + fInfo.Name()

			bu, err := ioutil.ReadFile(filepath)
			if err != nil {
				return err
			}

			restyConfig := make([]*elasticsearch.RestConfig, 0)
			elastics := make([]*elasticsearch.Elasticsearch, 0)
			if err := json.Unmarshal(bu, &restyConfig); err != nil {
				return err
			}

			for configIndex, _ := range restyConfig {
				if restyConfig[configIndex].IsZero() {
					return errors.New("method and path must be required")
				}

				restyConfig[configIndex].MigrationPath = path
				if err := restyConfig[configIndex].ReplaceStringWithIndex(index); err != nil {
					return err
				}
				if err := restyConfig[configIndex].ExcludeHeader(excludeHeader); err != nil {
					return err
				}
				elastics = append(elastics, &elasticsearch.Elasticsearch{
					Index:      index,
					RestConfig: restyConfig[configIndex],
				})
			}

			for _, elastic := range elastics {
				url := fmt.Sprintf("%s/%s", strings.Trim(database, "/"), strings.Trim(elastic.RestConfig.Path, "/"))
				req := resty.New().SetContentLength(true).SetDebug(debug).SetAllowGetMethodPayload(true).R()
				req.URL = url
				req.Method = elastic.RestConfig.Method
				req.Header = elastic.RestConfig.ToHTTPHeader()
				req.Body = elastic.RestConfig.Body

				resp, err := req.Send()
				if err != nil {
					return err
				}

				if resp.StatusCode() >= 400 {
					msg := errors.New(string(resp.Body()))
					if skippError {
						fmt.Println(msg.Error())
					} else {
						return msg
					}
				}
			}

			fmt.Println("migrate file: " + fInfo.Name() + " success")
		}
	}

	return nil
}

func seedUpHttpCmd(database string, path string, excludeHeader string, skippError bool, debug bool) error {
	filesInfo, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	if len(filesInfo) == 0 {
		return migrate.ErrNoChange
	}

	for _, fInfo := range filesInfo {
		if !fInfo.IsDir() && strings.Contains(fInfo.Name(), ".up.json") {
			filepath := path + "/" + fInfo.Name()

			bu, err := ioutil.ReadFile(filepath)
			if err != nil {
				return err
			}

			restyConfig := make([]*httpModels.RestConfig, 0)
			params := make([]map[string]interface{}, 0)
			if err := json.Unmarshal(bu, &params); err != nil {
				return err
			}
			for _, param := range params {
				r := httpModels.NewRestConfig(param)
				restyConfig = append(restyConfig, r)
			}

			for _, rest := range restyConfig {
				if err := rest.ExcludeHeader(excludeHeader); err != nil {
					return err
				}
				url := fmt.Sprintf("%s/%s", strings.Trim(database, "/"), strings.Trim(rest.Path, "/"))
				req := resty.New().SetContentLength(true).SetDebug(debug).SetAllowGetMethodPayload(true).R()
				req.URL = url
				req.QueryParam = rest.QueryParams
				req.Method = rest.Method
				req.Header = http.Header(rest.Header)
				req.Body = rest.Body

				if rest.BodyType == "binary" {
					ymlPath := path + "/" + rest.FilePath
					bu, err := ioutil.ReadFile(ymlPath)
					if err != nil {
						return err
					}

					req.SetBody(bytes.NewReader(bu))
				}

				resp, err := req.Send()
				if err != nil {
					return err
				}

				if resp.StatusCode() >= 400 {
					msg := errors.New(string(resp.Body()))
					if skippError {
						fmt.Println(msg.Error())
					} else {
						return msg
					}
				}
			}

			fmt.Println("migrate file: " + fInfo.Name() + " success")
		}
	}

	return nil
}

func seedDownHttpCmd(database string, path string, excludeHeader string, skippError bool, debug bool) error {
	filesInfo, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	if len(filesInfo) == 0 {
		return migrate.ErrNoChange
	}

	for i := len(filesInfo) - 1; i >= 0; i-- {
		fInfo := filesInfo[i]
		if !fInfo.IsDir() && strings.Contains(fInfo.Name(), ".down.json") {
			filepath := path + "/" + fInfo.Name()

			bu, err := ioutil.ReadFile(filepath)
			if err != nil {
				return err
			}

			restyConfig := make([]*httpModels.RestConfig, 0)
			params := make([]map[string]interface{}, 0)
			if err := json.Unmarshal(bu, &params); err != nil {
				return err
			}
			for _, param := range params {
				r := httpModels.NewRestConfig(param)
				restyConfig = append(restyConfig, r)
			}

			for _, rest := range restyConfig {
				if err := rest.ExcludeHeader(excludeHeader); err != nil {
					return err
				}
				url := fmt.Sprintf("%s/%s", strings.Trim(database, "/"), strings.Trim(rest.Path, "/"))
				req := resty.New().SetContentLength(true).SetDebug(debug).SetAllowGetMethodPayload(true).R()
				req.URL = url
				req.QueryParam = rest.QueryParams
				req.Method = rest.Method
				req.Header = http.Header(rest.Header)
				req.Body = rest.Body

				resp, err := req.Send()
				if err != nil {
					return err
				}

				if resp.StatusCode() >= 400 {
					msg := errors.New(string(resp.Body()))
					if skippError {
						fmt.Println(msg.Error())
					} else {
						return msg
					}
				}
			}

			fmt.Println("migrate file: " + fInfo.Name() + " success")
		}
	}

	return nil
}

func seedDownElasticCmd(database string, path string, excludeHeader string, index string, skippError bool, debug bool) error {
	filesInfo, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	if len(filesInfo) == 0 {
		return migrate.ErrNoChange
	}

	for i := len(filesInfo) - 1; i >= 0; i-- {
		fInfo := filesInfo[i]
		if !fInfo.IsDir() && strings.Contains(fInfo.Name(), ".down.json") {
			filepath := path + "/" + fInfo.Name()

			bu, err := ioutil.ReadFile(filepath)
			if err != nil {
				return err
			}

			restyConfig := make([]*elasticsearch.RestConfig, 0)
			elastics := make([]*elasticsearch.Elasticsearch, 0)
			if err := json.Unmarshal(bu, &restyConfig); err != nil {
				return err
			}

			for configIndex, _ := range restyConfig {
				if restyConfig[configIndex].IsZero() {
					return errors.New("method and path must be required")
				}

				restyConfig[configIndex].MigrationPath = path
				if err := restyConfig[configIndex].ReplaceStringWithIndex(index); err != nil {
					return err
				}
				if err := restyConfig[configIndex].ExcludeHeader(excludeHeader); err != nil {
					return err
				}
				elastics = append(elastics, &elasticsearch.Elasticsearch{
					Index:      index,
					RestConfig: restyConfig[configIndex],
				})
			}

			for _, elastic := range elastics {
				url := fmt.Sprintf("%s/%s", strings.Trim(database, "/"), strings.Trim(elastic.RestConfig.Path, "/"))
				req := resty.New().SetContentLength(true).SetDebug(debug).SetAllowGetMethodPayload(true).R()
				req.URL = url
				req.Method = elastic.RestConfig.Method
				req.Header = elastic.RestConfig.ToHTTPHeader()
				req.Body = elastic.RestConfig.Body

				resp, err := req.Send()
				if err != nil {
					return err
				}

				if resp.StatusCode() >= 400 {
					msg := errors.New(string(resp.Body()))
					if skippError {
						fmt.Println(msg.Error())
					} else {
						return msg
					}
				}
			}

			fmt.Println("migrate file: " + fInfo.Name() + " success")
		}
	}

	return nil
}

func downCmd(m *migrate.Migrate, limit int) error {
	if limit >= 0 {
		if err := m.Steps(-limit); err != nil {
			if err != migrate.ErrNoChange {
				return err
			}
			log.Println(err)
		}
	} else {
		if err := m.Down(); err != nil {
			if err != migrate.ErrNoChange {
				return err
			}
			log.Println(err)
		}
	}
	return nil
}

func dropCmd(m *migrate.Migrate) error {
	if err := m.Drop(); err != nil {
		return err
	}
	return nil
}

func forceCmd(m *migrate.Migrate, v int) error {
	if err := m.Force(v); err != nil {
		return err
	}
	return nil
}

func versionCmd(m *migrate.Migrate) error {
	v, dirty, err := m.Version()
	if err != nil {
		return err
	}
	if dirty {
		log.Printf("%v (dirty)\n", v)
	} else {
		log.Println(v)
	}
	return nil
}

// numDownMigrationsFromArgs returns an int for number of migrations to apply
// and a bool indicating if we need a confirm before applying
func numDownMigrationsFromArgs(applyAll bool, args []string) (int, bool, error) {
	if applyAll {
		if len(args) > 0 {
			return 0, false, errors.New("-all cannot be used with other arguments")
		}
		return -1, false, nil
	}

	switch len(args) {
	case 0:
		return -1, true, nil
	case 1:
		downValue := args[0]
		n, err := strconv.ParseUint(downValue, 10, 64)
		if err != nil {
			return 0, false, errors.New("can't read limit argument N")
		}
		return int(n), false, nil
	default:
		return 0, false, errors.New("too many arguments")
	}
}
