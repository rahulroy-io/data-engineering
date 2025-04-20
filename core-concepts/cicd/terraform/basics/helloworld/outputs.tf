#print hellow world

output "helloworld" {
  value = "hellowworld"
}

output "check_test_variable_value_string" {
  value = "the value is ${var.test_string}"
}

output "check_test_variable_value_number" {
  value = "the value is ${var.test_number}"
}

output "check_test_variable_list" {
  value = "element value is ${var.test_list[0]}"
}