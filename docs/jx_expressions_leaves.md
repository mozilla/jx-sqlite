

# Proving the correctness of the star selector


## Objective

This document is a dry attempt to prove the correctness of the star selection pattern. The proof is step-by-step explanation to show it is consistent with the other rules of JSON Query Expressions. It appeals to the reader' sense of pattern matching to be convincing.


### Data

We only need one complicated document to show the selector patterns


    {"a":{
        "b":{
            "c":1,
            "d":2
        },
        "e":{
            "f":3,
            "g":4
        }
    }}

## Problem

The star selector is not intuitive to implement. This has resulted in wrong tests, which directed the creation of incorrect code.  This is not a big problem right now (Aug2017) because the star selector is not used often. We should get it correct before we build too much with it. 

## Philosophy

The most important feature of the star selector is to flatten object structures to their leaves:

    "select":["*"] => {"a.b.c":1, "a.b.d":2, "a.e.f":3, "a.e.g":4}

this is to mimic the similar SQL feature. Everything else is open for debate.

The complexity of the star selector may come from the fact it is a type of meta-query operator: It changes the number of columns returned based on the database schema. Because it is a meta-query operator, it could be replaced with some query pre-processing; and this should not be problematic because JSON Query Expresssions are assumed to be built by some pre-processor already.  

## Current status

This is the current conclusion for this document. 

**List Format**
 
          "select":["*"] => {"a.b.c":1, "a.b.d":2, "a.e.f":3, "a.e.g":4} 
        "select":["a.*"] => {"a":{"b.c":1, "b.d":2, "e.f":3, "e.g":4}}
      "select":["a.b.*"] => {"a":{"b":{"c":1, "d":2}}}
    "select":["a.b.c.*"] => {"a":{"b":{"c":1}}}

**Table Format**

The use of star ("`*`") is now assumed to be the short form of the `leaves()` operator with a name of dot ("`.`"). This is different from a select clause that has an expression; which would require a name. This is also different from the dot selector, which assumes the name is the object selected.

          "select":["*"] => {
                                "header":["a.b.c", "a.b.d", "a.e.f", "a.e.g"], 
                                "data":[[1, 2, 3, 4]]
                            } 
        "select":["a.*"] => {
                                "header":["b.c", "b.d", "e.f", "e.g"], 
                                "data":{[1, 2, 3, 4]]
                            }
      "select":["a.b.*"] => {
                                "header":["c", "d"],
                                "data":{[1, 2]]
                            }
    "select":["a.b.c.*"] => {
                                "header":["."],
                                "data":[[1]]
                            }

The star selector has can avoid namespace collisions using two properties:

* `name` - which is common in all select clauses; it adds to the path of the resulting values. This is not good if you want the flattened structure to show up as additional columns.
* `prefix` - is a parameter of the `leaves()` operator, and can be given to the select clause: It adds a text prefix to each of the flattened column names. This allows you to define top-level properties, plus avoid namespace collisions.

example 

    "select":[{"value":"a.b.*", "prefix":"p_"}] 
  
results in 
 
    {
        "header":["p_c", "p_d"],
        "data":{[1, 2]]
    }

## List format

### Dot Selection

We review the select clause using simple values to review the effect of naming on a selection. This provides a pattern we can use to review the star selection permutations.


**Explicit Dot Object**

Selecting an array will return an object, with named parameters. In this case the property is named `"n"`.

    "select":[{"name":"n", "value":"."    }] => {"n":{"a":{"b":{"c":1,"d":2},"e":{"f":3,"g":4}}}}
    "select":[{"name":"n", "value":"a"    }] => {"n":     {"b":{"c":1,"d":2},"e":{"f":3,"g":4}} }
    "select":[{"name":"n", "value":"a.b"  }] => {"n":          {"c":1,"d":2}                    }
    "select":[{"name":"n", "value":"a.b.c"}] => {"n":               1                           }

**Explicit Dot Value**

List formatting without an array will ignore the select names, it returns only the values

    "select":{"name":"n", "value":"."    } => {"a":{"b":{"c":1,"d":2},"e":{"f":3,"g":4}}}
    "select":{"name":"n", "value":"a"    } =>      {"b":{"c":1,"d":2},"e":{"f":3,"g":4}}
    "select":{"name":"n", "value":"a.b"  } =>           {"c":1,"d":2}
    "select":{"name":"n", "value":"a.b.c"} =>                1

**Implicit Dot Value**

Leaving the name implied, without an array, gives the same result  

    "select":"."      => {"a":{"b":{"c":1,"d":2},"e":{"f":3,"g":4}}}
    "select":"a"      =>      {"b":{"c":1,"d":2},"e":{"f":3,"g":4}}
    "select":"a.b"    =>           {"c":1,"d":2}
    "select":"a.b.c"  =>                1

**Explicit Dot Value**

We could give any name we want, they will be ignored. Still the same result.

    "select":{"name":".",     "value":"."    } => {"a":{"b":{"c":1,"d":2},"e":{"f":3,"g":4}}}
    "select":{"name":"a",     "value":"a"    } =>      {"b":{"c":1,"d":2},"e":{"f":3,"g":4}}
    "select":{"name":"a.b",   "value":"a.b"  } =>           {"c":1,"d":2}
    "select":{"name":"a.b.c", "value":"a.b.c"} =>                1

**Explicit Dot Object**

Adding the array forces the names to be used as destination paths. This is the same result, but now the result is structured.

    "select":[{"name":".",     "value":"."    }] => {"a":{"b":{"c":1,"d":2},"e":{"f":3,"g":4}}}
    "select":[{"name":"a",     "value":"a"    }] => {"a":{"b":{"c":1,"d":2},"e":{"f":3,"g":4}}}
    "select":[{"name":"a.b",   "value":"a.b"  }] => {"a":{"b":{"c":1,"d":2}                  }}
    "select":[{"name":"a.b.c", "value":"a.b.c"}] => {"a":{"b":{"c":1      }                  }}

**Implicit Dot Object**

If the names are the same as the values, we can leave them out, for the same effect

    "select":["."    ] => {"a":{"b":{"c":1,"d":2},"e":{"f":3,"g":4}}}
    "select":["a"    ] => {"a":{"b":{"c":1,"d":2},"e":{"f":3,"g":4}}}
    "select":["a.b"  ] => {"a":{"b":{"c":1,"d":2}                  }}
    "select":["a.b.c"] => {"a":{"b":{"c":1      }                  }}


### Star Selection

We go through the same sequence with the star selector for the list format

**Explicit Star Object**

Selecting an array will return an object, with named parameters. The star will flatten any structure to the leaves.

    "select":[{"name":"n", "value":"*"      }] => {"n":{"a.b.c":1, "a.b.d":2, "a.e.f":3, "a.e.g":4}}
    "select":[{"name":"n", "value":"a.*"    }] => {"n":{  "b.c":1,   "b.d":2,   "e.f":3,   "e.g":4}}
    "select":[{"name":"n", "value":"a.b.*"  }] => {"n":{    "c":1,     "d":2                      }}
    "select":[{"name":"n", "value":"a.b.c.*"}] => {"n":         1                                  }

**Explicit Star Value**

List formatting, without an array, returns only the values

    "select":{"name":"n", "value":"*"      } => {"a.b.c":1, "a.b.d":2, "a.e.f":3, "a.e.g":4}
    "select":{"name":"n", "value":"a.*"    } => {  "b.c":1,   "b.d":2,   "e.f":3,   "e.g":4}
    "select":{"name":"n", "value":"a.b.*"  } => {    "c":1,     "d":2                      }
    "select":{"name":"n", "value":"a.b.c.*"} =>          1 

**Implicit Star Value**

Leaving the name implied, without an array, gives the same result  

    "select":"*"       => {"a.b.c":1, "a.b.d":2, "a.e.f":3, "a.e.g":4}
    "select":"a.*"     => {  "b.c":1,   "b.d":2,   "e.f":3,   "e.g":4}
    "select":"a.b.*"   => {    "c":1,     "d":2                      }
    "select":"a.b.c.*" =>          1 

**Explicit Star Value**

We could give any name we want, they will still be ignored. Still the same result.

    "select":{"name":".",     "value":"*"      } => {"a.b.c":1, "a.b.d":2, "a.e.f":3, "a.e.g":4}
    "select":{"name":"a",     "value":"a.*"    } => {  "b.c":1,   "b.d":2,   "e.f":3,   "e.g":4}
    "select":{"name":"a.b",   "value":"a.b.*"  } => {    "c":1,     "d":2                      }
    "select":{"name":"a.b.c", "value":"a.b.c.*"} =>          1

**Explicit Star Object**

Adding the array forces the names to be used as destination paths. This is the same values, but now the result is structured.

    "select":[{"name":".",     "value":"*"      }] =>                {"a.b.c":1, "a.b.d":2, "a.e.f":3, "a.e.g":4} 
    "select":[{"name":"a",     "value":"a.*"    }] => {"a":          {  "b.c":1,   "b.d":2,   "e.f":3,   "e.g":4}  }
    "select":[{"name":"a.b",   "value":"a.b.*"  }] => {"a":{"b":     {    "c":1,     "d":2                      } }}
    "select":[{"name":"a.b.c", "value":"a.b.c.*"}] => {"a":{"b":{"c":         1                                  }}}

**Implicit Star Object**

**THIS IS DIFFERENT: When the name is left out, it is assumed to be dot ("`.`")**

    "select":["*"      ] => {"a.b.c":1, "a.b.d":2, "a.e.f":3, "a.e.g":4}
    "select":["a.*"    ] => {  "b.c":1,   "b.d":2,   "e.f":3,   "e.g":4}
    "select":["a.b.*"  ] =>      "c":1,     "d":2                      }
    "select":["a.b.c.*"] =>          1                                  


## Table format

The star selector was created to generalize the SQL star: `SELECT * FROM my_table`. We want a way to declare a column for every primitive value; both to save time declaring all the possible columns, and leverage a dynamic selector that is relevant over a variety of different schema.

The table format uses the **top-level properties for column names only**. Inner properties are used to define the structure of compound columns.

* Define a prefix for the leaves, to avoid namespace collision  `{"prefix":"p", "value":"a.*"}`
* Put the flattened leaves into a single column as an inner object  `{"name":"n", "value":"a.*"}`


Without a name given to a select clause, we will assume the name is `"."`. This allows us to declare top-level properties easily.

    "select":["*"      ] => {"header":["a.b.c", "a.b.d", "a.e.f", "a.e.g"], "data":[[1, 2, 3, 4]]} 
    "select":["a.*"    ] => {"header":[  "b.c",   "b.d",   "e.f",   "e.g"], "data":[[1, 2, 3, 4]]}
    "select":["a.b.*"  ] => {"header":[    "c",     "d"                  ], "data":[[1, 2      ]]}
    "select":["a.b.c.*"] => {"header":[    "."                           ], "data":[[1         ]]}


The header names in a table are derived from the selector name, implied or not. The names are dot-delimted paths, with escaping is used for explicit dots.  


## Cube format

The cube format derives its column names from the table header. 
