cmake_minimum_required(VERSION 2.8)

PROJECT(aggregates)

# Path for yMySQL include directory
INCLUDE_DIRECTORIES("C:/SD/wamp/bin/mysql/mysql5.5.24/include")

ADD_DEFINITIONS("-DHAVE_DLOPEN")
ADD_LIBRARY(aggregates MODULE aggregates.c aggregates.def)
TARGET_LINK_LIBRARIES(aggregates wsock32)