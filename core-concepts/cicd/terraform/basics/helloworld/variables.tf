variable "test_string" {
  type = string
  default = "rahul"
}

variable "test_number" {
  type = string
  default = 100
}

variable "test_list" {
  type = list
  default = ["rahul", "pallavi"]
}

variable "age_lookup" {
  type = map
  default = {
    rahul = 30
    pallavi = 35
  }
}

variable "test_name" {
  type = string
}