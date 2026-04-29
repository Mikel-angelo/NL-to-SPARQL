# Query Run

- Run ID: `20260429-092537-730353`
- Run At: `2026-04-29T09:25:37.730353+00:00`
- Status: `completed`
- Dataset: `enovation-20260429-090134-362794`
- Endpoint: `http://127.0.0.1:3030/enovation-20260429-090134-362794/query`

## Question

What type of audiences exist?

## Retrieved Context

### Chunk 1: Audience

- Score: `1.0064969062805176`

```text
Class: Audience

Label: Audience

Description: Parent class of the different type of audiences

Object Properties:
- isSOPAudienceOf -> SOP
- isTrainingAudienceOf -> TrainingCourse

Datatype Properties:
- None
```

### Chunk 2: Students

- Score: `1.2958340644836426`

```text
Class: Students

Label: Students

Description: An audience category representing learners participating in training, educational programmes, or awareness activities related to crisis management

Object Properties:
- None

Datatype Properties:
- None
```

### Chunk 3: DecisionMakers

- Score: `1.3087642192840576`

```text
Class: DecisionMakers

Label: Desicion Makers

Description: An audience category representing senior officials with authority to make strategic decisions in crisis or emergency contexts

Object Properties:
- None

Datatype Properties:
- None
```

### Chunk 4: PrivateCompanies

- Score: `1.3160600662231445`

```text
Class: PrivateCompanies

Label: Private Companies

Description: An audience category representing private-sector organisations contributing to crisis preparedness, support activities, or infrastructure operations

Object Properties:
- None

Datatype Properties:
- None
```

### Chunk 5: FirstResponders

- Score: `1.3661415576934814`

```text
Class: FirstResponders

Label: First Responders

Description: An audience category representing frontline emergency responders directly involved in initial crisis operations

Object Properties:
- None

Datatype Properties:
- None
```

### Chunk 6: NationalMunicipalRegionalAuthorities

- Score: `1.389589548110962`

```text
Class: NationalMunicipalRegionalAuthorities

Label: National Municipal Regional Authorities

Description: An audience category representing public authorities at national, municipal, or regional level involved in governance, planning, or crisis management

Object Properties:
- None

Datatype Properties:
- None
```

### Chunk 7: InternationalOrganisations

- Score: `1.4013513326644897`

```text
Class: InternationalOrganisations

Label: International Organsations

Description: An audience category representing international bodies and agencies participating in crisis management, coordination, or support activities

Object Properties:
- None

Datatype Properties:
- None
```

### Chunk 8: FireRescueServices

- Score: `1.4087597131729126`

```text
Class: FireRescueServices

Label: Fire Rescue Services

Description: An audience category representing fire and rescue service personnel engaged in emergency response and incident management

Object Properties:
- None

Datatype Properties:
- None
```

### Chunk 9: SecondResponders

- Score: `1.4189202785491943`

```text
Class: SecondResponders

Label: Second Responders

Description: An audience category representing individuals or organisations providing follow-up support after initial emergency response actions

Object Properties:
- None

Datatype Properties:
- None
```

### Chunk 10: HealthHospitalMedicalPersonnel

- Score: `1.436194658279419`

```text
Class: HealthHospitalMedicalPersonnel

Label: Health Hospital Medical Personnel

Description: An audience category representing medical, hospital, and healthcare staff involved in emergency care, crisis response, or public health management

Object Properties:
- None

Datatype Properties:
- None
```

## Generation Prompt

```text
System Role:
You are an expert SPARQL query generator. Use only the provided ontology context and URIs. Do not invent classes, properties, or namespaces.

Relevant Ontology Chunks:
Ontology label, not a SPARQL prefix: enovation
Dataset label, not a SPARQL prefix: enovation-20260429-090134-362794
Chunk 1:
"""
Class: Audience

Label: Audience

Description: Parent class of the different type of audiences

Object Properties:
- isSOPAudienceOf -> SOP
- isTrainingAudienceOf -> TrainingCourse

Datatype Properties:
- None
"""
Chunk 2:
"""
Class: Students

Label: Students

Description: An audience category representing learners participating in training, educational programmes, or awareness activities related to crisis management

Object Properties:
- None

Datatype Properties:
- None
"""
Chunk 3:
"""
Class: DecisionMakers

Label: Desicion Makers

Description: An audience category representing senior officials with authority to make strategic decisions in crisis or emergency contexts

Object Properties:
- None

Datatype Properties:
- None
"""
Chunk 4:
"""
Class: PrivateCompanies

Label: Private Companies

Description: An audience category representing private-sector organisations contributing to crisis preparedness, support activities, or infrastructure operations

Object Properties:
- None

Datatype Properties:
- None
"""
Chunk 5:
"""
Class: FirstResponders

Label: First Responders

Description: An audience category representing frontline emergency responders directly involved in initial crisis operations

Object Properties:
- None

Datatype Properties:
- None
"""
Chunk 6:
"""
Class: NationalMunicipalRegionalAuthorities

Label: National Municipal Regional Authorities

Description: An audience category representing public authorities at national, municipal, or regional level involved in governance, planning, or crisis management

Object Properties:
- None

Datatype Properties:
- None
"""
Chunk 7:
"""
Class: InternationalOrganisations

Label: International Organsations

Description: An audience category representing international bodies and agencies participating in crisis management, coordination, or support activities

Object Properties:
- None

Datatype Properties:
- None
"""
Chunk 8:
"""
Class: FireRescueServices

Label: Fire Rescue Services

Description: An audience category representing fire and rescue service personnel engaged in emergency response and incident management

Object Properties:
- None

Datatype Properties:
- None
"""
Chunk 9:
"""
Class: SecondResponders

Label: Second Responders

Description: An audience category representing individuals or organisations providing follow-up support after initial emergency response actions

Object Properties:
- None

Datatype Properties:
- None
"""
Chunk 10:
"""
Class: HealthHospitalMedicalPersonnel

Label: Health Hospital Medical Personnel

Description: An audience category representing medical, hospital, and healthcare staff involved in emergency care, crisis response, or public health management

Object Properties:
- None

Datatype Properties:
- None
"""

Auto-Generated Prefix Declarations:
PREFIX : <http://www.semanticweb.org/eNOVATION-ontology#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX grddl: <http://www.w3.org/2003/g/data-view#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

Prefix Usage Rules:
- Use only the prefix declarations listed above.
- Do not use the ontology label or dataset label as a prefix.
- If a default prefix declaration is listed as `PREFIX : <...>`, use terms such as `:ClassName` for that namespace.
- Unknown prefixes will fail validation.

Optional Few-Shot Examples:
No few-shot examples provided.

Output Format Instructions:
Return only one valid SPARQL query. Use either full URIs in angle brackets or the provided prefix declarations. Only prefixes listed under Auto-Generated Prefix Declarations are allowed. Ontology and dataset names are labels, not SPARQL prefixes. Use the ':' prefix for terms in the default ontology namespace when it is listed. Do not invent prefixes, classes, properties, or namespaces. Do not include explanations, markdown fences, or extra text.

User Question:
What type of audiences exist?
```

## Initial Generated Query

```sparql
SELECT ?audienceType WHERE {
  ?audienceType rdf:type :Audience .
}
```

## Attempts

### Iteration 1: completed

- Validation: `VALIDATION_OK`

#### Query

```sparql
SELECT ?audienceType WHERE {
  ?audienceType rdf:type :Audience .
}
```

#### Validation Stages

- `syntactic`: passed (`SPARQL_PARSE_OK`)
- `prefix`: passed (`PREFIXES_OK`)
- `vocabulary`: passed (`VOCABULARY_OK`)
- `structural`: passed (`STRUCTURE_OK`)

#### Execution

- passed (`EXECUTION_OK`)

#### Errors

- None

## Final Query

```sparql
PREFIX : <http://www.semanticweb.org/eNOVATION-ontology#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX grddl: <http://www.w3.org/2003/g/data-view#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?audienceType WHERE {
  ?audienceType rdf:type :Audience .
}
```

## Execution Result

```json
{
  "head": {
    "vars": [
      "audienceType"
    ]
  },
  "results": {
    "bindings": [
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_CivilProtection"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_CrisisManagers"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_DecisionMakers"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_FirstRescueServices"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_FirstResponders"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_HealthPersonnel"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_InternationalOrganisations"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_MilitaryDefenceExperts"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_NationalRegionalAuthorities"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_PoliceAgents"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_PrivateCompanies"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_SecondResponders"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_Students"
        }
      },
      {
        "audienceType": {
          "type": "uri",
          "value": "http://www.semanticweb.org/eNOVATION-ontology#Aud_TechnologyOperators"
        }
      }
    ]
  }
}
```

## Errors

- None
