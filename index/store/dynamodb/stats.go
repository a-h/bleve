package dynamodb

import "encoding/json"

type stats struct {
	store *Store
}

func (s *stats) MarshalJSON() ([]byte, error) {
	//TODO: Update the code to track the number of API calls made, and the number of read/write units consumed.
	bs := map[string]interface{}{}
	return json.Marshal(bs)
}
