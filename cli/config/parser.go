package main

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type ParserType int

const (
	NONE ParserType = iota
	CLUSTER
	SERVICE_GROUP
)

type PxfServiceGroup struct {
	Name  string
	Ports []int
	IsSsl bool
}

type PxfCluster struct {
	Name       string
	Collocated bool
	Hosts      []PxfHost
	Endpoint   string
	Groups     map[string]*PxfServiceGroup
}

type PxfHost struct {
	Hostname string
}

func GetFileContents(path string) ([]string, error) {
	var fileContents []string

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fileContents = append(fileContents, line)
	}

	if err := file.Close(); err != nil {
		return nil, err
	}

	return fileContents, nil
}

func GetParserTypeAndFieldName(line string) (ParserType, string) {
	tokens := strings.Split(line, ".")

	var val ParserType
	switch tokens[0] {
	case "cluster":
		val = CLUSTER
	case "group":
		val = SERVICE_GROUP
	default:
		val = NONE
	}

	fmt.Printf("type: %d, field: %s\n", val, tokens[len(tokens)-1])

	return val, tokens[len(tokens)-1]
}
func UpdateFieldByName(obj any, fieldName string, value any) error {
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(fieldName)

	if !structFieldValue.IsValid() {
		return fmt.Errorf("no such field: %s in obj", fieldName)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("cannot set %s field value", fieldName)
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		return fmt.Errorf("provided value type didn't match obj field type")
	}

	structFieldValue.Set(val)
	return nil
}

// Function to convert string to specified type
func parseValue(input string) any {
	// Check if the input string is empty
	if input == "" {
		return nil
	}

	// Convert the input string to lowercase for case-insensitive comparison
	input = strings.ToLower(input)

	// Check if the input string represents an integer
	if num, err := strconv.Atoi(input); err == nil {
		return num
	}

	// Check if the input string is "true" or "false"
	if input == "true" {
		return true
	} else if input == "false" {
		return false
	}

	// If none of the above conditions match, return the input string itself
	return input
}

func parseIntArrayValue(input string) []int {
	arrayString := strings.Split(input, ",")

	var parsedArray []int
	for _, x := range arrayString {
		parsedArray = append(parsedArray, parseValue(x).(int))
	}

	return parsedArray
}

func main() {
	structToKeyMap := map[string]string{
		"name":       "Name",
		"ports":      "Ports",
		"collocated": "Collocated",
		"ssl":        "IsSsl",
	}

	fileContent, err := GetFileContents("../../server/pxf-service/src/templates/cluster/cluster.txt")
	if err != nil {
		println(err)
	}

	group := &PxfServiceGroup{
		Name:  "default",
		Ports: nil,
		IsSsl: false,
	}

	cluster := &PxfCluster{
		Name:       "",
		Collocated: false,
		Hosts:      nil,
		Endpoint:   "",
		Groups:     nil,
	}

	enumToStructMap := map[ParserType]any{
		CLUSTER:       cluster,
		SERVICE_GROUP: group,
	}

	for _, line := range fileContent {
		// keyAndValue[0] is the enum and keyString
		// keyAndValue[1] is the userDefinedValue
		keyAndValue := strings.Split(line, "=")
		fmt.Println(keyAndValue[0])
		parserType, keyString := GetParserTypeAndFieldName(keyAndValue[0])

		var userDefinedValue any
		if strings.Contains(keyAndValue[1], ",") {
			userDefinedValue = parseIntArrayValue(keyAndValue[1])
			fmt.Println(userDefinedValue)
		} else {
			userDefinedValue = parseValue(keyAndValue[1])
			fmt.Println(userDefinedValue)

		}

		err := UpdateFieldByName(enumToStructMap[parserType], structToKeyMap[keyString], userDefinedValue)
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println()
	}

	fmt.Printf("%+v\n", cluster)
	fmt.Printf("%+v\n", group)
}
