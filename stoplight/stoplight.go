/* Copyright (C) 2023 Vivien Roggero LLC - All Rights Reserved
 */
package stoplight

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type Stoplight struct {
	client *http.Client
	ctx    context.Context

	collection *base.Collection
}

func init() {
	base.RegisterDriver(base.StoplightType, NewStoplight)
	base.RegisterTestConnectionFunc(base.StoplightType, TestStoplight)
}

// NewStoplight returns configured Stoplight driver instance
func NewStoplight(ctx context.Context, sourceConfig *base.SourceConfig, collection *base.Collection) (base.Driver, error) {
	config := &StoplightConfig{}
	err := jsonutils.UnmarshalConfig(sourceConfig.Config, config)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}

	return &Stoplight{
		client: client,
		ctx:    ctx,
		collection: collection,
	}, nil
}

// TestStoplight tests connection to Stoplight without creating Driver instance
func TestStoplight(sourceConfig *base.SourceConfig) error {
	config := &StoplightConfig{}
	err := jsonutils.UnmarshalConfig(sourceConfig.Config, config)
	if err != nil {
		return err
	}

	client := &http.Client{}

	req, err := http.NewRequest("GET", fmt.Sprintf("https://services.leadconnectorhq.com/calendars/%s", config.CalendarId), nil)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", config.AccessToken)
	req.Header.Add("Version", config.ApiVersion)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Stoplight returned status code %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var calendar map[string]interface{}
	err = json.Unmarshal(body, &calendar)
	if err != nil {
		return err
	}

	if calendar == nil {
		return fmt.Errorf("Stoplight returned empty response")
	}

	return nil
}

func (s *Stoplight) GetCollectionTable() string {
	return s.collection.GetTableName()
}

func (s *Stoplight) GetCollectionMetaKey() string {
	return s.collection.Name + "_" + s.GetCollectionTable()
}

func (s *Stoplight) GetRefreshWindow() (time.Duration, error) {
	return time.Hour * 24 * 31, nil
}

func (s *Stoplight) ReplaceTables() bool {
	return false
}

func (s *Stoplight) GetObjectsFor(interval *base.TimeInterval, objectsLoader base.ObjectsLoader) error {
	// Get the calendars for the given interval.
	calendars, err := s.GetCalendars()
	if err != nil {
		return err
	}

	// Get the contacts for the given interval.
	contacts, err := s.GetContacts()
	if err != nil {
		return err
	}

	// Get the opportunities for the given interval.
	opportunities, err := s.GetOpportunities()
	if err != nil {
		return err
	}

	// Load the objects into the database.
	return objectsLoader.Load(interval, calendars, contacts, opportunities)
}

func (s *Stoplight) GetCalendars() error {
	// Get the calendars from the API endpoint.
	req, err := http.NewRequest("GET", "https://services.leadconnectorhq.com/calendars/", nil)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", s.config.AccessToken)
	req.Header.Add("Version", s.config.ApiVersion)

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Stoplight returned status code %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Parse the JSON response.
	var calendars []map[string]interface{}
	err = json.Unmarshal(body, &calendars)
	if err != nil {
		return err
	}

	// Return the calendars.
	return nil
}

func (s *Stoplight) GetContacts() error {
	// Get the contacts from the API endpoint.
	req, err := http.NewRequest("GET", "https://services.leadconnectorhq.com/contacts/", nil)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", s.config.AccessToken)
	req.Header.Add("Version", s.config.ApiVersion)

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Stoplight returned status code %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Parse the JSON response.
	var contacts []map[string]interface{}
	err = json.Unmarshal(body, &contacts)
	if err != nil {
		return err
	}

	// Return the contacts.
	return nil
}

func (s *Stoplight) GetOpportunities() error {
	// Get the opportunities from the API endpoint.
	req, err := http.NewRequest("GET", "https://services.leadconnectorhq.com/opportunities/", nil)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", s.config.AccessToken)
	req.Header.Add("Version", s.config.ApiVersion)

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Stoplight returned status code %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Parse the JSON response.
	var opportunities []map[string]interface{}
	err = json.Unmarshal(body, &opportunities)
	if err != nil {
		return err
	}

	// Return the opportunities.
	return nil
}

