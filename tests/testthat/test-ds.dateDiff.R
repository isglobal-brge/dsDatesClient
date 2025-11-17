#
# Set up
#

context("ds.dateDiff::dslite::setup")

#
# Tests using DSLite
#

context("ds.dateDiff::dslite::difference in days (default)")
test_that("ds.dateDiff computes difference in days", {
  # Create test data with dates
  person <- data.frame(
    person_id = 1:3,
    birth_date = as.Date(c("1990-01-15", "1991-06-20", "1992-12-25")),
    visit_date = as.Date(c("2020-01-15", "2021-06-20", "2022-12-25"))
  )
  
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE
  )
  
  assign("dslite.server", builder, envir = .GlobalEnv)
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  conns <- list(server1 = conn)
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test function with default unit (days)
  ds.dateDiff(
    time1 = "person$birth_date",
    time2 = "person$visit_date",
    unit = "days",
    newobj = "age_days",
    add_as_column = FALSE,
    datasources = conns
  )
  
  result <- DSI::datashield.aggregate(conns, quote(age_days))
  
  expect_true(is.numeric(result[[1]]))
  expect_length(result[[1]], 3)
  # All should be approximately 30 years * 365.25 = 10957.5 days
  expect_true(all(result[[1]] > 10950 & result[[1]] < 10965))
  
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.dateDiff::dslite::difference in months")
test_that("ds.dateDiff computes difference in months", {
  # Create test data with dates
  person <- data.frame(
    person_id = 1:3,
    birth_date = as.Date(c("1990-01-15", "1991-06-20", "1992-12-25")),
    visit_date = as.Date(c("2020-01-15", "2021-06-20", "2022-12-25"))
  )
  
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE
  )
  
  assign("dslite.server", builder, envir = .GlobalEnv)
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  conns <- list(server1 = conn)
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test function with months unit
  ds.dateDiff(
    time1 = "person$birth_date",
    time2 = "person$visit_date",
    unit = "months",
    newobj = "age_months",
    add_as_column = FALSE,
    datasources = conns
  )
  
  result <- DSI::datashield.aggregate(conns, quote(age_months))
  
  expect_true(is.numeric(result[[1]]))
  expect_length(result[[1]], 3)
  # All should be approximately 30 years * 12 = 360 months
  expect_true(all(result[[1]] > 358 & result[[1]] < 362))
  
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.dateDiff::dslite::difference in years")
test_that("ds.dateDiff computes difference in years", {
  # Create test data with dates
  person <- data.frame(
    person_id = 1:3,
    birth_date = as.Date(c("1990-01-15", "1991-06-20", "1992-12-25")),
    visit_date = as.Date(c("2020-01-15", "2021-06-20", "2022-12-25"))
  )
  
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE
  )
  
  assign("dslite.server", builder, envir = .GlobalEnv)
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  conns <- list(server1 = conn)
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test function with years unit
  ds.dateDiff(
    time1 = "person$birth_date",
    time2 = "person$visit_date",
    unit = "years",
    newobj = "age_years",
    add_as_column = FALSE,
    datasources = conns
  )
  
  result <- DSI::datashield.aggregate(conns, quote(age_years))
  
  expect_true(is.numeric(result[[1]]))
  expect_length(result[[1]], 3)
  # All should be approximately 30 years
  expect_true(all(result[[1]] > 29.5 & result[[1]] < 30.5))
  
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.dateDiff::dslite::error handling - invalid unit")
test_that("ds.dateDiff throws error for invalid unit", {
  person <- data.frame(
    person_id = 1:3,
    birth_date = as.Date(c("1990-01-15", "1991-06-20", "1992-12-25")),
    visit_date = as.Date(c("2020-01-15", "2021-06-20", "2022-12-25"))
  )
  
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE
  )
  
  assign("dslite.server", builder, envir = .GlobalEnv)
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  conns <- list(server1 = conn)
  DSI::dsAssignTable(conn, "person", "person")
  
  expect_error(
    ds.dateDiff(
      time1 = "person$birth_date",
      time2 = "person$visit_date",
      unit = "invalid_unit",
      datasources = conns
    ),
    "\\[unit\\] must be one of: 'days', 'months', or 'years'"
  )
  
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.dateDiff::dslite::default unit is days")
test_that("ds.dateDiff uses days as default unit", {
  person <- data.frame(
    person_id = 1:2,
    birth_date = as.Date(c("1990-01-15", "1991-06-20")),
    visit_date = as.Date(c("2020-01-15", "2021-06-20"))
  )
  
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE
  )
  
  assign("dslite.server", builder, envir = .GlobalEnv)
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  conns <- list(server1 = conn)
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test without specifying unit (should default to days)
  ds.dateDiff(
    time1 = "person$birth_date",
    time2 = "person$visit_date",
    newobj = "age_default",
    add_as_column = FALSE,
    datasources = conns
  )
  
  result <- DSI::datashield.aggregate(conns, quote(age_default))
  
  expect_true(is.numeric(result[[1]]))
  expect_length(result[[1]], 2)
  # Should be in days (large numbers, around 10957)
  expect_true(all(result[[1]] > 10950 & result[[1]] < 10965))
  
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.dateDiff::dslite::as_integer parameter - days")
test_that("ds.dateDiff truncates to integer for days", {
  person <- data.frame(
    person_id = 1:3,
    birth_date = as.Date(c("1990-01-15", "1991-06-20", "1992-12-25")),
    visit_date = as.Date(c("2020-01-15", "2021-06-20", "2022-12-25"))
  )
  
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE
  )
  
  assign("dslite.server", builder, envir = .GlobalEnv)
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  conns <- list(server1 = conn)
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test with as_integer = TRUE
  ds.dateDiff(
    time1 = "person$birth_date",
    time2 = "person$visit_date",
    unit = "days",
    as_integer = TRUE,
    newobj = "age_days_int",
    add_as_column = FALSE,
    datasources = conns
  )
  
  result <- DSI::datashield.aggregate(conns, quote(age_days_int))
  
  expect_true(is.numeric(result[[1]]))
  expect_true(all(result[[1]] == as.integer(result[[1]])))  # All values should be integers
  expect_length(result[[1]], 3)
  expect_true(all(result[[1]] > 10950 & result[[1]] < 10965))
  
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.dateDiff::dslite::as_integer parameter - months")
test_that("ds.dateDiff truncates to integer for months", {
  person <- data.frame(
    person_id = 1:3,
    birth_date = as.Date(c("1990-01-15", "1991-06-20", "1992-12-25")),
    visit_date = as.Date(c("2020-01-15", "2021-06-20", "2022-12-25"))
  )
  
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE
  )
  
  assign("dslite.server", builder, envir = .GlobalEnv)
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  conns <- list(server1 = conn)
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test with as_integer = TRUE for months
  ds.dateDiff(
    time1 = "person$birth_date",
    time2 = "person$visit_date",
    unit = "months",
    as_integer = TRUE,
    newobj = "age_months_int",
    add_as_column = FALSE,
    datasources = conns
  )
  
  result <- DSI::datashield.aggregate(conns, quote(age_months_int))
  
  expect_true(is.numeric(result[[1]]))
  expect_true(all(result[[1]] == as.integer(result[[1]])))  # All values should be integers
  expect_length(result[[1]], 3)
  expect_true(all(result[[1]] >= 359 & result[[1]] <= 360))
  
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

context("ds.dateDiff::dslite::as_integer parameter - years")
test_that("ds.dateDiff truncates to integer for years", {
  person <- data.frame(
    person_id = 1:3,
    birth_date = as.Date(c("1990-01-15", "1991-06-20", "1992-12-25")),
    visit_date = as.Date(c("2020-01-15", "2021-06-20", "2022-12-25"))
  )
  
  config <- DSLite::defaultDSConfiguration(include = c("dsDates", "dsBase"))
  builder <- DSLite::newDSLiteServer(
    tables = list(person = person),
    config = config,
    strict = FALSE
  )
  
  assign("dslite.server", builder, envir = .GlobalEnv)
  conn <- DSI::dsConnect(DSLite::DSLite(), name = "server1", url = "dslite.server")
  assign("server1", conn, envir = .GlobalEnv)
  conns <- list(server1 = conn)
  DSI::dsAssignTable(conn, "person", "person")
  
  # Test with as_integer = TRUE for years
  ds.dateDiff(
    time1 = "person$birth_date",
    time2 = "person$visit_date",
    unit = "years",
    as_integer = TRUE,
    newobj = "age_years_int",
    add_as_column = FALSE,
    datasources = conns
  )
  
  result <- DSI::datashield.aggregate(conns, quote(age_years_int))
  
  expect_true(is.numeric(result[[1]]))
  expect_true(all(result[[1]] == as.integer(result[[1]])))  # All values should be integers
  expect_length(result[[1]], 3)
  expect_true(all(result[[1]] == 29 | result[[1]] == 30))  # Should be 29 or 30 years
  
  DSI::datashield.logout(conns)
  DSI::dsDisconnect(conn)
  if(exists("dslite.server", envir = .GlobalEnv)) rm(dslite.server, envir = .GlobalEnv)
  if(exists("server1", envir = .GlobalEnv)) rm(server1, envir = .GlobalEnv)
})

#
# Done
#

context("ds.dateDiff::dslite::shutdown")

context("ds.dateDiff::dslite::done")

