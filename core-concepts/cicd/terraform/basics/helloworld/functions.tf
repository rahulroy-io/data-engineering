output "test_function_upper" {
  value = "upper function output is ${upper(var.test_string)}"
}

output "test_function_join" {
  value = join(" and ", var.test_list)
}

output "looup_value" {
  value = "my name is ${var.test_name} and my age is ${lookup(var.age_lookup, var.test_name)}"
}