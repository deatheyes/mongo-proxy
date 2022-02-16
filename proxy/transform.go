package proxy

import (
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// TransformFunc parse and transform request and reponse for
// shielding the deployment of mongo and logic of router.
type TransformFunc func(w *Worker, document bsoncore.Document) bsoncore.Document

// |any community driver|          |    mongo-porxy      |       |    mongo   |
// |    user client     |   <->    |upstream<->downstream|  <->  |target cluster|
// Wrap*:   upstream -> downstream
// Revert*: downstream -> upstream

// WrapCommandDatabase wrap the element specified by '$db'
func WrapCommandDatabase(w *Worker, document bsoncore.Document) bsoncore.Document {
	return TransformElement(document, "$db", func(e1 []byte) []byte {
		dbElement := bsoncore.Element(e1)
		if err := dbElement.Validate(); err != nil {
			return e1
		}
		if dbElement.Value().Type != bsontype.String {
			return e1
		}
		return bsoncore.AppendStringElement(nil, "$db", w.targetDB)
	})
}

// WrapListCollection tranform all nesessary embeded documents, such 'filter', 'name' and 'index', for 'listcollection' response.
func WrapListCollection(w *Worker, document bsoncore.Document) bsoncore.Document {
	doc := TransformElement(document, "filter", func(e1 []byte) []byte {
		element := bsoncore.Element(e1)
		if err := element.Validate(); err != nil {
			return e1
		}
		if element.Value().Type != bsontype.EmbeddedDocument {
			return e1
		}
		filterElements, err := element.Value().Document().Elements()
		if err != nil {
			return e1
		}
		elementList := [][]byte{bsoncore.AppendRegexElement(nil, "name", "^"+w.UID+"_", "")}
		for _, elem := range filterElements {
			elementList = append(elementList, elem)
		}
		return bsoncore.BuildDocumentElement(nil, "filter", elementList...)
	})
	return WrapCommandDatabase(w, doc)
}

func WrapGetMore(w *Worker, documemt bsoncore.Document) bsoncore.Document {
	doc := TransformElement(documemt, "collection", func(e1 []byte) []byte {
		collectionElement := bsoncore.Element(e1)
		if err := collectionElement.Validate(); err != nil {
			return e1
		}
		if collectionElement.Value().Type != bsontype.String {
			return e1
		}
		collection := collectionElement.Value().StringValue()
		return bsoncore.AppendStringElement(nil, "collection", WrapCollection(w.UID, collection))
	})
	return WrapCommandDatabase(w, doc)
}

// WrapCommand is the embeded transform function which wraps the 'collection' value and '$db' field.
func WrapCommand(key string) TransformFunc {
	return func(w *Worker, documemt bsoncore.Document) bsoncore.Document {
		// wrap collection
		doc := TransformElement(documemt, key, func(e1 []byte) []byte {
			element := bsoncore.Element(e1)
			if err := element.Validate(); err != nil {
				return e1
			}
			if element.Value().Type != bsontype.String {
				return e1
			}
			return bsoncore.AppendStringElement(nil, key, WrapCollection(w.UID, element.Value().StringValue()))
		})
		// wrap database
		return WrapCommandDatabase(w, doc)
	}
}

// RevertCursor erases or transform cursor's fileds that expose the deployment of mongo.
func RevertCursor(w *Worker, documemt bsoncore.Document) bsoncore.Document {
	return TransformElement(documemt, "cursor", func(e1 []byte) []byte {
		cursorElem := bsoncore.Element(e1)
		if err := cursorElem.Validate(); err != nil {
			return e1
		}
		if cursorElem.Value().Type != bsontype.EmbeddedDocument {
			return e1
		}
		listCollections := false
		cursorElemValue := TransformElement(cursorElem.Value().Document(), "ns", func(e2 []byte) []byte {
			nsElem := bsoncore.Element(e2)
			if err := nsElem.Validate(); err != nil {
				return e2
			}
			if nsElem.Value().Type != bsontype.String {
				return e2
			}
			ns, err := RevertNamespace(w.UID, nsElem.Value().StringValue())
			if err != nil {
				return e2
			}
			if strings.HasSuffix(strings.ToLower(ns), "listcollections") {
				listCollections = true
			}
			return bsoncore.AppendStringElement(nil, "ns", ns)
		})
		// database commnads are more complicated than those of collection.
		// TODO: more database commands.
		if listCollections {
			cursorElemValue = RevertBatchElement(w, cursorElemValue, "firstBatch")
			cursorElemValue = RevertBatchElement(w, cursorElemValue, "nextBatch")
		}
		return bsoncore.AppendDocumentElement(nil, "cursor", cursorElemValue)
	})
}

// RevertBatchElement erases or transform cursor for database commands.
// eg. a typical listCollection command:
/*
{
    "cursor": {
        "id": {
            "$numberLong": "0"
        },
        "ns": "mongo01.$cmd.listCollections",
        "firstBatch": [
            {
                "name": "xxxuid_todo",
                "type": "collection",
				...
                "idIndex": {
					...
                    "ns": "mongo01.xxxuid_todo"
                }
            }
        ]
    },
	...
}
transformed:
{
    "cursor": {
        "id": {
            "$numberLong": "0"
        },
        "ns": "xxxuid.$cmd.listCollections",
        "firstBatch": [
            {
                "name": "todo",
                "type": "collection",
				...
                "idIndex": {
					...
                    "ns": "xxxuid.todo"
                }
            }
        ]
    },
	...
}
*/
func RevertBatchElement(w *Worker, documemt bsoncore.Document, key string) bsoncore.Document {
	return TransformElement(documemt, key, func(e1 []byte) []byte {
		batchElement := bsoncore.Element(e1)
		if err := batchElement.Validate(); err != nil {
			return e1
		}
		if batchElement.Value().Type != bsontype.Array {
			return e1
		}
		values, err := batchElement.Value().Array().Values()
		if err != nil {
			return e1
		}
		arrayElementList := []bsoncore.Value{}
		for _, v := range values {
			if err := v.Validate(); err != nil {
				return e1
			}
			if v.Type != bsontype.EmbeddedDocument {
				return e1
			}
			wrapV := RevertBathItem(w, v.Document())
			arrayElementList = append(arrayElementList, bsoncore.Value{
				Type: bsontype.EmbeddedDocument,
				Data: wrapV,
			})
		}
		array := bsoncore.BuildArray(nil, arrayElementList...)
		return bsoncore.AppendArrayElement(nil, key, array)
	})
}

func RevertBathItem(w *Worker, documemt bsoncore.Document) bsoncore.Document {
	doc := TransformElement(documemt, "name", func(e1 []byte) []byte {
		nameElement := bsoncore.Element(e1)
		if err := nameElement.Validate(); err != nil {
			return e1
		}
		if nameElement.Value().Type != bsontype.String {
			return e1
		}
		name := nameElement.Value().StringValue()
		name = strings.TrimPrefix(name, w.UID+"_")
		return bsoncore.AppendStringElement(nil, "name", name)
	})
	return TransformElement(doc, "idIndex", func(e2 []byte) []byte {
		idIndexElement := bsoncore.Element(e2)
		if err := idIndexElement.Validate(); err != nil {
			return e2
		}
		if idIndexElement.Value().Type != bsontype.EmbeddedDocument {
			return e2
		}
		idIndexDocument := RevertNamespaceElement(w, idIndexElement.Value().Document())
		return bsoncore.AppendDocumentElement(nil, "idIndex", idIndexDocument)
	})
}

func RevertNamespaceElement(w *Worker, documemt bsoncore.Document) bsoncore.Document {
	return TransformElement(documemt, "ns", func(e1 []byte) []byte {
		nsEelement := bsoncore.Element(e1)
		if err := nsEelement.Validate(); err != nil {
			return e1
		}
		if nsEelement.Value().Type != bsontype.String {
			return e1
		}
		ns, err := RevertNamespace(w.UID, nsEelement.Value().StringValue())
		if err != nil {
			return e1
		}
		return bsoncore.AppendStringElement(nil, "ns", ns)
	})
}

// helper fuctions
func WrapCollection(UID, collection string) string {
	if []byte(collection)[0] == '$' {
		// do not wrap buildin command
		return collection
	}
	return UID + "_" + collection
}

// RevertNamespace transform mongo namespace into client side one.
// eg. mongo01.mongo01_todo => ttboegefv.todo
func RevertNamespace(UID, ns string) (string, error) {
	parts := strings.Split(ns, ".")
	if len(parts) < 2 {
		return "", errors.New("unexpected namespance")
	}
	parts[0] = UID
	parts[1] = strings.TrimPrefix(parts[1], UID+"_")
	return strings.Join(parts, "."), nil
}

// TransformElement try to find and wrap the elemnt specified by 'key'.
// return the source data if come up with any errors.
func TransformElement(document bsoncore.Document, key string, wrap func([]byte) []byte) bsoncore.Document {
	elements, err := document.Elements()
	if err != nil {
		return document
	}

	idx := -1
	for i := 0; i < len(elements); i++ {
		if strings.ToLower(elements[i].Key()) == strings.ToLower(key) {
			idx = i
			break
		}
	}
	if idx < 0 {
		// not found
		return document
	}
	newElements := [][]byte{}
	for i := 0; i < idx; i++ {
		newElements = append(newElements, elements[i])
	}
	newElements = append(newElements, wrap(elements[idx]))
	for i := idx + 1; i < len(elements); i++ {
		newElements = append(newElements, elements[i])
	}
	return bsoncore.BuildDocumentFromElements(nil, newElements...)
}
