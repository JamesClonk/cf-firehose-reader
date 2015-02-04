package main

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/cloudfoundry/noaa"
	"github.com/cloudfoundry/noaa/events"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("usage: cf-firehose-reader <username> <password>")
		fmt.Println("  - user needs to have 'doppler.firehose' scope")
		os.Exit(1)
	}

	token, err := getToken(os.Args[1], os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

	readFirehose(token)
}

func readFirehose(token string) {
	doppler := `wss://doppler.10.244.0.34.xip.io:443`
	id := "firehose-reader"
	conn := noaa.NewConsumer(doppler, &tls.Config{InsecureSkipVerify: true}, nil)

	msgs := make(chan *events.Envelope)
	defer close(msgs)

	errs := make(chan error)
	defer close(errs)

	go conn.Firehose(id, token, msgs, errs, nil)

	for {
		select {
		case msg := <-msgs:
			fmt.Printf("%+v\n", msg)
		case err := <-errs:
			fmt.Fprintf(os.Stderr, "%v\n", err.Error())
		}
	}
}

func getToken(username, password string) (string, error) {
	tokenUrl := `https://login.10.244.0.34.xip.io/oauth/token`

	data := url.Values{}
	data.Set("grant_type", "password")
	data.Set("scope", "")
	data.Set("username", username)
	data.Set("password", password)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	req, err := http.NewRequest("POST", tokenUrl, bytes.NewBufferString(data.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte("cf:"))))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != 200 {
		return "", errors.New(string(body))
	}

	type ErrorResponse struct {
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}

	type AuthResponse struct {
		AccessToken  string        `json:"access_token"`
		TokenType    string        `json:"token_type"`
		RefreshToken string        `json:"refresh_token"`
		ExpiresIn    int           `json:"expires_in"`
		Scope        string        `json:"scope"`
		Error        ErrorResponse `json:"error"`
	}

	var response AuthResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", err
	}

	if response.Error.Error != "" {
		return "", errors.New(fmt.Sprintf("%s %s", response.Error.Error, response.Error.ErrorDescription))
	}

	if !strings.Contains(response.Scope, "doppler.firehose") {
		return "", errors.New(fmt.Sprintf("%s does not have doppler.firehose scope", username))
	}

	return fmt.Sprintf("%s %s", response.TokenType, response.AccessToken), nil
}
