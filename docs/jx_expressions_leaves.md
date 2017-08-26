

# Proving the correctness of the star selector


## Objective

This document is attempt to prove the correctness of the star selection pattern. The proof is step-by-step explanation to show it is consistent with the other rules of JSON Query Expressions. It appeals to the reader' sense of pattern matching to be convincing.


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



## List format

Only the list formatting is proved here. It is hoped the `table` and `cube` are natural derivations (more below). 

### Dot Selection


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

We could give any name we want, they will be ignored 

    "select":{"name":".",     "value":"."    } => {"a":{"b":{"c":1,"d":2},"e":{"f":3,"g":4}}}
    "select":{"name":"a",     "value":"a"    } =>      {"b":{"c":1,"d":2},"e":{"f":3,"g":4}}
    "select":{"name":"a.b",   "value":"a.b"  } =>           {"c":1,"d":2}
    "select":{"name":"a.b.c", "value":"a.b.c"} =>                1

**Explicit Dot Object**

Adding the array forces the names to be used as destination paths. This is the same result, but now it is being structured.

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

We could give any name we want, they will still be ignored 

    "select":{"name":".",     "value":"*"      } => {"a.b.c":1, "a.b.d":2, "a.e.f":3, "a.e.g":4}
    "select":{"name":"a",     "value":"a.*"    } => {  "b.c":1,   "b.d":2,   "e.f":3,   "e.g":4}
    "select":{"name":"a.b",   "value":"a.b.*"  } => {    "c":1,     "d":2                      }
    "select":{"name":"a.b.c", "value":"a.b.c.*"} =>          1

**Explicit Star Object**

Adding the array forces the names to be used as destination paths. This is the same result, but now it is being structured.

    "select":[{"name":".",     "value":"*"      }] =>                {"a.b.c":1, "a.b.d":2, "a.e.f":3, "a.e.g":4} 
    "select":[{"name":"a",     "value":"a.*"    }] => {"a":          {  "b.c":1,   "b.d":2,   "e.f":3,   "e.g":4}  }
    "select":[{"name":"a.b",   "value":"a.b.*"  }] => {"a":{"b":     {    "c":1,     "d":2                      } }}
    "select":[{"name":"a.b.c", "value":"a.b.c.*"}] => {"a":{"b":{"c":         1                                  }}}

**Implicit Star Object**

If the names are the same as the values, we can leave them out, for the same effect

    "select":["*"      ] =>                {"a.b.c":1, "a.b.d":2, "a.e.f":3, "a.e.g":4} 
    "select":["a.*"    ] => {"a":          {  "b.c":1,   "b.d":2,   "e.f":3,   "e.g":4}  }
    "select":["a.b.*"  ] => {"a":{"b":     {    "c":1,     "d":2                      } }}
    "select":["a.b.c.*"] => {"a":{"b":{"c":         1                                  }}}


## Table format

The header names in a table are derived from the selector name, implied or not. The names are dot-delimted paths, with escaping is used for explicit dots

**Explicit Star Object**

Adding the array forces the names to be used as destination paths. This is the same result, but now it is being structured.

    "select":["*"      ] => {"header":["a\.b\.c", "a\.b\.d", "a\.e\.f", "a\.e\.g"], "data":[[1, 2, 3, 4]]} 
    "select":["a.*"    ] => {"header":[ "a.b\.c",  "a.b\.d",  "a.e\.f",  "a.e\.g"], "data":[[1, 2, 3, 4]]}
    "select":["a.b.*"  ] => {"header":[  "a.b.c",   "a.b.d"                      ], "data":[[1, 2      ]]}
    "select":["a.b.c.*"] => {"header":[  "a.b.c"                                 ], "data":[[1         ]]}


## Cube format

The cube format deives its column names from the table header. No more explanation is required.
