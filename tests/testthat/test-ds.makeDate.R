#
# Set up
#

context("ds.makeDate::dslite::setup")

#
# Tests using DSLite
#

context("ds.makeDate::dslite::year only, Date format")
test_that("ds.makeDate with year only, Date format", {
  # Create test data
  person <- data.frame(
    year_of_birth = c(1990, 1991, 1992, 1993),
    person_id = 1:4
  )
  
  # Setup DSLite server with dsDates package configuration
  # Note: dsDates package must be installed and available on the server side
  # Include dsBase to allow isDefined function to work
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE  # Allow more functions for testing
  )
  
  # Register the server with a name (required for DSLite)
  assign("dslite.server", builder, envir = .GlobalEnv)
  
  # Connect to the server
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  
  # Use connection directly (simpler approach)
  conns <- list(server1 = conn)
  
  # Assign the table from server to R session
  # The table is already in the server, we just need to assign it to the R session
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test function
  ds.makeDate(
    year.name = "person$year_of_birth",
    format = "Date",
    newobj = "birth_date",
    add_as_column = FALSE,
    datasources = conns
  )
  
  # Retrieve and verify result
  result <- DSI::datashield.aggregate(conns, quote(birth_date))
  
  expect_true(all("Date" %in% class(result[[1]])))
  expect_length(result[[1]], 4)
  expect_equal(as.character(result[[1]][1]), "1990-01-01")
  expect_equal(as.character(result[[1]][2]), "1991-01-01")
  expect_equal(as.character(result[[1]][3]), "1992-01-01")
  expect_equal(as.character(result[[1]][4]), "1993-01-01")
  
  # Logout and cleanup
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.makeDate::dslite::year, month, day, Date format")
test_that("ds.makeDate with year, month, day, Date format", {
  # Create test data - OMOP CDM person table format
  person <- data.frame(
    person_id = 1:3,
    year_of_birth = c(1990, 1991, 1992),
    month_of_birth = c(6, 7, 8),
    day_of_birth = c(15, 20, 25)
  )
  
  # Setup DSLite server with dsDates package configuration
  # Note: dsDates package must be installed and available on the server side
  # Include dsBase to allow isDefined function to work
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE  # Allow more functions for testing
  )
  
  # Register the server with a name (required for DSLite)
  assign("dslite.server", builder, envir = .GlobalEnv)
  
  # Connect to the server
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  
  # Use connection directly (simpler approach)
  conns <- list(server1 = conn)
  
  # Assign the table from server to R session
  # The table is already in the server, we just need to assign it to the R session
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test function - OMOP CDM use case
  ds.makeDate(
    year.name = "person$year_of_birth",
    month.name = "person$month_of_birth",
    day.name = "person$day_of_birth",
    format = "Date",
    newobj = "birth_date",
    add_as_column = FALSE,
    datasources = conns
  )
  
  # Retrieve and verify result
  result <- DSI::datashield.aggregate(conns, quote(birth_date))
  
  expect_true(all("Date" %in% class(result[[1]])))
  expect_length(result[[1]], 3)
  expect_equal(as.character(result[[1]][1]), "1990-06-15")
  expect_equal(as.character(result[[1]][2]), "1991-07-20")
  expect_equal(as.character(result[[1]][3]), "1992-08-25")
  
  # Logout and cleanup
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.makeDate::dslite::add as column to table")
test_that("ds.makeDate adding result as column to table", {
  # Create test data - OMOP CDM person table format
  person <- data.frame(
    person_id = 1:3,
    year_of_birth = c(1990, 1991, 1992),
    month_of_birth = c(6, 7, 8),
    day_of_birth = c(15, 20, 25)
  )
  
  # Setup DSLite server with dsDates package configuration
  # Note: dsDates package must be installed and available on the server side
  # Include dsBase to allow isDefined function to work
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE  # Allow more functions for testing
  )
  
  # Register the server with a name (required for DSLite)
  assign("dslite.server", builder, envir = .GlobalEnv)
  
  # Connect to the server
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  
  # Use connection directly (simpler approach)
  conns <- list(server1 = conn)
  
  # Assign the table from server to R session
  # The table is already in the server, we just need to assign it to the R session
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test function with add_as_column = TRUE
  ds.makeDate(
    year.name = "person$year_of_birth",
    month.name = "person$month_of_birth",
    day.name = "person$day_of_birth",
    format = "Date",
    newobj = "birth_date",
    add_as_column = TRUE,
    datasources = conns
  )
  
  # Retrieve and verify result - check that column was added
  person_result <- DSI::datashield.aggregate(conns, quote(person))
  
  expect_true("birth_date" %in% names(person_result[[1]]))
  expect_true(all("Date" %in% class(person_result[[1]]$birth_date)))
  expect_length(person_result[[1]]$birth_date, 3)
  expect_equal(as.character(person_result[[1]]$birth_date[1]), "1990-06-15")
  expect_equal(as.character(person_result[[1]]$birth_date[2]), "1991-07-20")
  expect_equal(as.character(person_result[[1]]$birth_date[3]), "1992-08-25")
  
  # Verify temporary object was cleaned up
  # The standalone object should not exist (it was removed after adding to table)
  # We can verify this by checking that accessing it directly would fail
  # (This is an optional check - the main verification is that the column exists in the table)
  
  # Logout and cleanup
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.makeDate::dslite::NA handling")
test_that("ds.makeDate handles NA values correctly", {
  # Create test data with NA values
  person <- data.frame(
    person_id = 1:3,
    year_of_birth = c(1990, NA, 1992),
    month_of_birth = c(6, 7, NA),
    day_of_birth = c(15, 20, 25)
  )
  
  # Setup DSLite server with dsDates package configuration
  # Note: dsDates package must be installed and available on the server side
  # Include dsBase to allow isDefined function to work
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE  # Allow more functions for testing
  )
  
  # Register the server with a name (required for DSLite)
  assign("dslite.server", builder, envir = .GlobalEnv)
  
  # Connect to the server
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  
  # Use connection directly (simpler approach)
  conns <- list(server1 = conn)
  
  # Assign the table from server to R session
  # The table is already in the server, we just need to assign it to the R session
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test function
  ds.makeDate(
    year.name = "person$year_of_birth",
    month.name = "person$month_of_birth",
    day.name = "person$day_of_birth",
    format = "Date",
    newobj = "birth_date",
    add_as_column = FALSE,
    datasources = conns
  )
  
  # Retrieve and verify result
  result <- DSI::datashield.aggregate(conns, quote(birth_date))
  
  expect_true(all("Date" %in% class(result[[1]])))
  expect_length(result[[1]], 3)
  expect_equal(as.character(result[[1]][1]), "1990-06-15")
  expect_true(is.na(result[[1]][2]))  # Year is NA
  expect_equal(as.character(result[[1]][3]), "1992-01-25")  # Month NA defaults to 1
  
  # Logout and cleanup
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.makeDate::dslite::POSIXct format")
test_that("ds.makeDate with POSIXct format", {
  # Create test data with time components
  visit <- data.frame(
    visit_id = 1:3,
    year = c(2020, 2021, 2022),
    month = c(1, 6, 12),
    day = c(15, 20, 25),
    hour = c(10, 14, 18),
    minute = c(30, 45, 15)
  )
  
  # Setup DSLite server with dsDates package configuration
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(visit = visit),
    config = config,
    strict = FALSE
  )
  
  # Register the server with a name (required for DSLite)
  assign("dslite.server", builder, envir = .GlobalEnv)
  
  # Connect to the server
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  
  # Use connection directly (simpler approach)
  conns <- list(server1 = conn)
  
  # Assign the table from server to R session
  DSI::dsAssignTable(conn, "visit", "visit")
  
  # Test function with POSIXct format
  ds.makeDate(
    year.name = "visit$year",
    month.name = "visit$month",
    day.name = "visit$day",
    hour.name = "visit$hour",
    minute.name = "visit$minute",
    format = "POSIXct",
    newobj = "visit_datetime",
    add_as_column = FALSE,
    datasources = conns
  )
  
  # Retrieve and verify result
  result <- DSI::datashield.aggregate(conns, quote(visit_datetime))
  
  expect_true(all("POSIXct" %in% class(result[[1]])))
  expect_length(result[[1]], 3)
  expect_equal(format(result[[1]][1], "%Y-%m-%d %H:%M:%S"), "2020-01-15 10:30:00")
  expect_equal(format(result[[1]][2], "%Y-%m-%d %H:%M:%S"), "2021-06-20 14:45:00")
  expect_equal(format(result[[1]][3], "%Y-%m-%d %H:%M:%S"), "2022-12-25 18:15:00")
  
  # Logout and cleanup
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.makeDate::dslite::year only with defaults")
test_that("ds.makeDate with year only uses month/day defaults", {
  # Create test data
  person <- data.frame(
    person_id = 1:3,
    year_of_birth = c(1990, 1991, 1992)
  )
  
  # Setup DSLite server with dsDates package configuration
  # Note: dsDates package must be installed and available on the server side
  # Include dsBase to allow isDefined function to work
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE  # Allow more functions for testing
  )
  
  # Register the server with a name (required for DSLite)
  assign("dslite.server", builder, envir = .GlobalEnv)
  
  # Connect to the server
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  
  # Use connection directly (simpler approach)
  conns <- list(server1 = conn)
  
  # Assign the table from server to R session
  # The table is already in the server, we just need to assign it to the R session
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test function with only year
  ds.makeDate(
    year.name = "person$year_of_birth",
    format = "Date",
    newobj = "birth_date",
    add_as_column = FALSE,
    datasources = conns
  )
  
  # Retrieve and verify result
  result <- DSI::datashield.aggregate(conns, quote(birth_date))
  
  expect_true(all("Date" %in% class(result[[1]])))
  expect_length(result[[1]], 3)
  # All should be January 1st (defaults)
  expect_equal(as.character(result[[1]][1]), "1990-01-01")
  expect_equal(as.character(result[[1]][2]), "1991-01-01")
  expect_equal(as.character(result[[1]][3]), "1992-01-01")
  
  # Logout and cleanup
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.makeDate::dslite::error handling - invalid format")
test_that("ds.makeDate throws error for invalid format", {
  # Create test data
  person <- data.frame(
    person_id = 1:3,
    year_of_birth = c(1990, 1991, 1992)
  )
  
  # Setup DSLite server with dsDates package configuration
  # Note: dsDates package must be installed and available on the server side
  # Include dsBase to allow isDefined function to work
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE  # Allow more functions for testing
  )
  
  # Register the server with a name (required for DSLite)
  assign("dslite.server", builder, envir = .GlobalEnv)
  
  # Connect to the server
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  
  # Use connection directly (simpler approach)
  conns <- list(server1 = conn)
  
  # Assign the table from server to R session
  # The table is already in the server, we just need to assign it to the R session
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test function with invalid format
  expect_error(
    ds.makeDate(
      year.name = "person$year_of_birth",
      format = "InvalidFormat",
      newobj = "birth_date",
      datasources = conns
    ),
    "\\[format\\] must be either 'Date' or 'POSIXct'"
  )
  
  # Logout and cleanup
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

#
# Done
#

context("ds.makeDate::dslite::shutdown")

context("ds.makeDate::dslite::done")

