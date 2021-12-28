package main

import (
	"bufio"
	"crypto/md5"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

type Message struct {
	UserProfile UserProfile `json:"user_profile"`
	ID          string      `json:"user"`
	Replies     []Reply     `json:"replies"`
	Text        string      `json:"text"`
	Ts          string      `json:"ts"`
}

type UserProfile struct {
	ID                string  `json:"id"`
	RealName          string  `json:"real_name"`
	Deleted           bool    `json:"deleted"`
	IsBot             bool    `json:"is_bot"`
	IsRestricted      bool    `json:"is_restricted"`
	IsUltraRestricted bool    `json:"is_ultra_restricted"`
	Profile           Profile `json:"profile"`
}

type Profile struct {
	Email string `json:"email"`
}

type Reply struct {
	ID string `json:"user"`
	Ts string `json:"ts"`
}

func main() {
	obfuscateNames := false
	minimumConnectedThreshold := 100
	if os.Getenv("OBFUSCATE_NAMES") == "true" {
		obfuscateNames = true
	}
	if os.Getenv("MINIMUM_WORD_COUNT") != "" {
		minimumConnectedThreshold, _ = strconv.Atoi(os.Getenv("MINIMUM_WORD_COUNT"))
	}

	userIDToNameLookup := loadUserIDJsonIntoMap()
	userIDToDepartmentLookup, hasNames := loadUserIDToDepartmentMap(false)

	userScores := map[string]map[string]int{}
	departmentScores := map[string]map[string]int{}

	dirs := loadDirsInData()
	for _, dir := range dirs {
		files := loadFilesInDir(dir)
		for _, file := range files {
			contents := loadFile(dir, file)
			var messages []Message
			json.Unmarshal(contents, &messages)
			for _, message := range messages {
				if message.ID != "" && len(message.Replies) > 0 {
					for _, reply := range message.Replies {
						if hasNames[message.ID] {
							name := userIDToNameLookup[message.ID]
							nameTalksTo := userIDToNameLookup[reply.ID]
							if _, ok := userScores[name]; !ok {
								userScores[name] = map[string]int{}
							}
							userScores[name][nameTalksTo] += len(strings.Split(message.Text, " ")) + countTargetMessageWords(messages, reply.Ts)

							dept := userIDToDepartmentLookup[message.ID]
							deptTalksTo := userIDToDepartmentLookup[reply.ID]
							if _, ok := departmentScores[dept]; !ok {
								departmentScores[dept] = map[string]int{}
							}
							departmentScores[dept][deptTalksTo] += len(strings.Split(message.Text, " ")) + countTargetMessageWords(messages, reply.Ts)
						}
					}
				}
			}
		}
	}

	os.RemoveAll("./output")
	os.Mkdir("./output", 0777)
	// open people for writing and create a csv writer
	people, err := os.Create("output/people.csv")
	if err != nil {
		panic(err)
	}
	defer people.Close()
	depts, err := os.Create("output/departments.csv")
	if err != nil {
		panic(err)
	}
	defer depts.Close()

	fmt.Fprintf(people, "%s,%s,%s\n", "name", "talks to", "word count")
	fmt.Fprintf(depts, "%s,%s,%s\n", "department", "talks to", "word count")
	for ID := range userScores {
		for ID2 := range userScores {
			if ID == ID2 {
				continue
			}
			name1 := ID
			name2 := ID2

			if obfuscateNames {
				// md5 name
				name1 = fmt.Sprintf("n%x", md5.Sum([]byte(ID)))[0:8]
				name2 = fmt.Sprintf("n%x", md5.Sum([]byte(ID2)))[0:8]
			}

			if userScores[ID][ID2] > minimumConnectedThreshold {
				fmt.Fprintf(people, "%s,%s,%d\n", name1, name2, userScores[ID][ID2]+userScores[ID2][ID])
			}
		}
	}

	for ID := range departmentScores {
		for ID2 := range departmentScores {
			if ID == ID2 {
				continue
			}
			if departmentScores[ID][ID2] > minimumConnectedThreshold {
				fmt.Fprintf(depts, "%s,%s,%d\n", ID, ID2, departmentScores[ID][ID2]+departmentScores[ID2][ID])
			}
		}
	}

}

func countTargetMessageWords(messages []Message, ts string) int {
	for _, message := range messages {
		if message.Ts == ts {
			return len(strings.Split(message.Text, " "))
		}
	}
	return 0
}

func loadUserIDToDepartmentMap(excludeExecs bool) (map[string]string, map[string]bool) {

	result := map[string]string{}
	result2 := map[string]bool{}

	// load the user json file
	contents2, err := ioutil.ReadFile("data/users.json")
	if err != nil {
		panic(err)
	}

	// create a map to hold the user ids to names
	emailToUserIdLookup := make(map[string]string)

	// unmarshal the json into an array of user profiles
	var userProfiles []UserProfile
	json.Unmarshal(contents2, &userProfiles)

	// loop through all the user profiles
	for _, userProfile := range userProfiles {
		if !userProfile.Deleted && !userProfile.IsBot && !userProfile.IsRestricted && !userProfile.IsUltraRestricted {
			// add the user id and name to the map
			email := strings.ToLower(userProfile.Profile.Email)
			emailToUserIdLookup[email] = userProfile.ID
		}
	}

	dept := 1
	email := 4

	f, err := os.Open("data/Company Directory _ Org Chart - Searchspring.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		panic(err)
	}

	for i, record := range records {
		if i == 0 {
			continue
		}
		if record[email] != "" {
			currentEmail := strings.ToLower(record[email])
			currentDepartment := record[dept]

			result[emailToUserIdLookup[currentEmail]] = currentDepartment
			result2[emailToUserIdLookup[currentEmail]] = true
		}
	}

	// if ignore file exists load it and get the user ids to ignore from the emails
	if _, err := os.Stat("data/ignore.txt"); err == nil {
		ignoreFile, err := os.Open("data/ignore.txt")
		if err != nil {
			panic(err)
		}
		defer ignoreFile.Close()

		scanner := bufio.NewScanner(ignoreFile)
		for scanner.Scan() {
			currentEmail := strings.ToLower(scanner.Text())
			if userId, ok := emailToUserIdLookup[currentEmail]; ok {
				delete(result2, userId)
				fmt.Printf("ignoring %s: %s\n", currentEmail, userId)
			}
		}
	}
	if err != nil {
		panic(err)
	}

	return result, result2
}

func loadUserIDJsonIntoMap() map[string]string {

	// load the user json file
	contents, err := ioutil.ReadFile("data/users.json")
	if err != nil {
		panic(err)
	}

	// create a map to hold the user ids to names
	userIdToNameLookup := make(map[string]string)

	// unmarshal the json into an array of user profiles
	var userProfiles []UserProfile
	json.Unmarshal(contents, &userProfiles)

	// loop through all the user profiles
	for _, userProfile := range userProfiles {
		if !userProfile.Deleted && !userProfile.IsBot && !userProfile.IsRestricted && !userProfile.IsUltraRestricted {
			// add the user id and name to the map
			userIdToNameLookup[userProfile.ID] = strings.ReplaceAll(userProfile.RealName, ",", "")
		}
	}

	// return the map
	return userIdToNameLookup
}

func loadFile(dir string, file string) []byte {

	// load the file
	contents, err := ioutil.ReadFile("data/" + dir + "/" + file)
	if err != nil {
		panic(err)
	}

	// return the file contents
	return contents
}
func loadFilesInDir(dir string) []string {

	// load all the files in the directory
	files, err := ioutil.ReadDir("data/" + dir)
	if err != nil {
		panic(err)
	}

	// create a slice to hold the file names
	var fileNames []string

	// loop through all the files
	for _, file := range files {

		// if the file is a file
		if !file.IsDir() {

			// add the file name to the slice
			fileNames = append(fileNames, file.Name())
		}
	}

	// return the slice of file names
	return fileNames
}

func loadDirsInData() []string {

	dirs, err := ioutil.ReadDir("data")
	if err != nil {
		panic(err)
	}

	// create a slice to hold the directory names
	var dirNames []string

	// loop through all the directories
	for _, dir := range dirs {

		// if the directory is a directory
		if dir.IsDir() {

			// add the directory name to the slice
			dirNames = append(dirNames, dir.Name())
		}
	}

	// return the slice of directory names
	return dirNames
}
