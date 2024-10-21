# Define the database connection to be used for this model.
connection: "starburst_connection"

include: "/views/**/*.view"



datagroup: datagroup_name {
  max_cache_age: "12 hour"
}

persist_with: datagroup_name
