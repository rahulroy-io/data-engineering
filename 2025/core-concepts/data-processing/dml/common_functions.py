# %%
from pyspark.sql import SparkSession

# %%

# DATE CONVERSION

# date conversion date -> string 
# y: year (1 or 2 digits)
# yy: year (2 digits)
# yyyy: year (4 digits)
# M: month (1 or 2 digits)
# MM: month (2 digits)
# MMM: abbreviated month name (e.g., Jan, Feb, Mar)
# MMMM: full month name (e.g., January, February, March)
# d: day of the month (1 or 2 digits)
# dd: day of the month (2 digits)
# ddd: abbreviated day of the week (e.g., Mon, Tue, Wed)
# dddd: full day of the week (e.g., Monday, Tuesday, Wednesday)
# h: hour (1 or 2 digits, 12-hour clock)
# hh: hour (2 digits, 12-hour clock)
# H: hour (1 or 2 digits, 24-hour clock)
# HH: hour (2 digits, 24-hour clock)
# m: minute (1 or 2 digits)
# mm: minute (2 digits)
# s: second (1 or 2 digits)
# ss: second (2 digits)
# S: millisecond (1 digit)
# SS: millisecond (2 digits)
# SSS: millisecond (3 digits)
# a: AM/PM marker
# Z: time zone offset (e.g., +0500, -0800)


