# Prototype of the Policy Engine

The intent of this prototype is to test the scalability of the Policy Engine against large number of policies. 

## Policy Indexing

Our approach for run-time policy evaluation is to use a boolean expression indexing mechanism to index the policies and stream the data tuples against it (pub-sub approach for policy evaluation). 

## Policy Guards

Guards is an approximation of the complete DNF policy expression which is evaluated as part of the query. They reduce the number of tuples that are evaluated by the policy index while ensuring there are no false positives (you can think of them as a sophisticated bloomfilter).

## Policy Simulator

We utilize a seed of different types of policies with their instances each and generate a large policy data set to test the policy engine.


## Policy Model
```json
{
  "id": "001",
  "description": "Policy example 1",
  "metadata": "",
  "object_conditions": [
    {
    "attribute": "Presence.timeStamp",
    "predicates": [{
      "value": "5 pm",
      "operator": ">="
    },
      {
        "value": "7 pm",
        "operator": "<="
      }
    ]
  },
    {
      "attribute": "Presence.user",
      "predicates": [{
        "value": "Alice",
        "operator": "="
      }
      ]
    }
  ],
  "querier_conditions": [
    {
    "attribute": "Query.timeStamp",
    "predicates": [
      {
      "value": "6 pm",
      "operator": ">="
      },
      {
        "value": "8 pm",
        "operator": "<="
      }
    ]
  },
    {
      "attribute": "Query.user_name",
      "predicates": [
        {
          "value": "John",
          "operator": "="
        }
      ]
    }
  ],
  "purpose": "",
  "action": ""
}
```
