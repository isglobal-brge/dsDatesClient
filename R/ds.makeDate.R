#' @title Create Date or POSIXct from Separate Year/Month/Day Columns
#'
#' @description Creates a Date or POSIXct object from separate numeric year, month, and day columns
#' on the server-side. This function is particularly useful for OMOP CDM data where birth dates are 
#' stored as separate \code{year_of_birth}, \code{month_of_birth}, and \code{day_of_birth} columns 
#' in the person table. The function handles missing values gracefully by using defaults (month/day = 1, 
#' hour/minute/second = 0) and can optionally add the resulting date as a new column to an existing table.
#'
#' @param year.name a character string providing the name of the year column/vector to be used (mandatory).
#' @param month.name a character string providing the name of the month column/vector (optional, defaults to NULL → uses 1).
#' @param day.name a character string providing the name of the day column/vector (optional, defaults to NULL → uses 1).
#' @param hour.name a character string providing the name of the hour column/vector (optional, defaults to NULL → uses 0).
#' @param minute.name a character string providing the name of the minute column/vector (optional, defaults to NULL → uses 0).
#' @param second.name a character string providing the name of the second column/vector (optional, defaults to NULL → uses 0).
#' @param format a character string specifying the output format: "Date" (default) or "POSIXct".
#' @param table.name a character string providing the name of the table to which the result should be added as a column (optional).
#' @param newobj a character string that provides the name for the output object that is stored on the data servers.
#' Default \code{"makeDate.newobj"}.
#' @param add_as_column A logical value to indicate if the converted values are to be added as a column to a dataframe.
#' This option requires that either \code{table.name} is provided or all input columns come from the same table.
#' The column name will be \code{newobj}. Defaults to \code{FALSE}.
#' @template datasources
#'
#' @return Does not have a return object.
#'
#' @details
#' **NA Handling:**
#' The function handles missing values gracefully:
#' - If \code{year} is NA, the result is NA (cannot create a date without a year)
#' - If \code{month} or \code{day} are NA but year exists, defaults are used (month=1, day=1)
#' - If \code{hour/minute/second} are NA but date components exist, defaults are used (0)
#' 
#' **Adding as Column:**
#' When \code{add_as_column=TRUE}:
#' - If \code{table.name} is provided, the result is added to that table
#' - Otherwise, if all input columns come from the same table (detected via "$" in column names), 
#'   that table is automatically used
#' - If inputs come from different tables or standalone vectors, \code{table.name} must be explicitly provided
#' 
#' **OMOP CDM Usage:**
#' For OMOP CDM person table with birth date columns:
#' \code{ds.makeDate(year.name = "person$year_of_birth", month.name = "person$month_of_birth", 
#' day.name = "person$day_of_birth", format = "Date", newobj = "birth_date", add_as_column = TRUE)}
#'
#' @examples
#' \dontrun{
#' # ============================================
#' # OMOP CDM Example: Create birth date from person table
#' # ============================================
#' # In OMOP CDM, birth dates are stored as separate columns:
#' # - person$year_of_birth (numeric)
#' # - person$month_of_birth (numeric, can be NA)
#' # - person$day_of_birth (numeric, can be NA)
#' 
#' # Create birth_date column and add it to the person table
#' ds.makeDate(
#'   year.name = "person$year_of_birth",
#'   month.name = "person$month_of_birth",
#'   day.name = "person$day_of_birth",
#'   format = "Date",
#'   newobj = "birth_date",
#'   add_as_column = TRUE
#' )
#' # Result: person table now has a new column "birth_date" with Date values
#' # Missing month/day values default to 1 (January 1st)
#' # Missing year values result in NA dates
#' 
#' # ============================================
#' # Alternative: Create as standalone vector (not added to table)
#' # ============================================
#' ds.makeDate(
#'   year.name = "person$year_of_birth",
#'   month.name = "person$month_of_birth",
#'   day.name = "person$day_of_birth",
#'   format = "Date",
#'   newobj = "birth_date_vector",
#'   add_as_column = FALSE  # Default
#' )
#' # Result: Creates a standalone vector "birth_date_vector" on the server
#' 
#' # ============================================
#' # Year only (month/day default to 1)
#' # ============================================
#' ds.makeDate(
#'   year.name = "person$year_of_birth",
#'   format = "Date",
#'   newobj = "birth_date_year_only"
#' )
#' # Result: Dates will be YYYY-01-01 (January 1st of each year)
#' 
#' # ============================================
#' # POSIXct format with time components
#' # ============================================
#' # For datetime columns (e.g., visit_start_datetime)
#' ds.makeDate(
#'   year.name = "visit$year",
#'   month.name = "visit$month",
#'   day.name = "visit$day",
#'   hour.name = "visit$hour",
#'   minute.name = "visit$minute",
#'   format = "POSIXct",
#'   newobj = "visit_datetime",
#'   add_as_column = TRUE
#' )
#' # Result: Creates POSIXct datetime with specified time components
#' # Missing time components default to 0
#' 
#' # ============================================
#' # Using explicit table.name parameter
#' # ============================================
#' # When inputs come from different sources or you want to specify the target table
#' ds.makeDate(
#'   year.name = "year_vec",  # Standalone vector
#'   month.name = "person$month_of_birth",  # From table
#'   day.name = "person$day_of_birth",  # From table
#'   format = "Date",
#'   newobj = "birth_date",
#'   table.name = "person",  # Explicitly specify target table
#'   add_as_column = TRUE
#' )
#' }
#'
#' @export
ds.makeDate <- function(year.name, 
                        month.name = NULL, 
                        day.name = NULL,
                        hour.name = NULL,
                        minute.name = NULL,
                        second.name = NULL,
                        format = "Date",
                        table.name = NULL,
                        newobj = NULL,
                        add_as_column = FALSE,
                        datasources = NULL){
  
  if (is.null(datasources)) {
    datasources <- DSI::datashield.connections_find()
  }

  if(!(is.list(datasources) && all(unlist(lapply(datasources, function(d) {methods::is(d,"DSConnection")}))))){
    stop("The 'datasources' were expected to be a list of DSConnection-class objects", call.=FALSE)
  }

  # Validate format
  if(!format %in% c("Date", "POSIXct")){
    stop("[format] must be either 'Date' or 'POSIXct'", call.=FALSE)
  }

  # Check if inputs are defined (this will catch non-existent objects/columns early)
  tryCatch({
    dsBaseClient:::isDefined(datasources, year.name)
  }, error = function(e) {
    stop(paste0("Cannot access '", year.name, "'. The object or column does not exist on the server. Please verify the object/column name is correct."), call. = FALSE)
  })
  
  if(!is.null(month.name)){
    tryCatch({
      dsBaseClient:::isDefined(datasources, month.name)
    }, error = function(e) {
      stop(paste0("Cannot access '", month.name, "'. The object or column does not exist on the server. Please verify the object/column name is correct."), call. = FALSE)
    })
  }
  if(!is.null(day.name)){
    tryCatch({
      dsBaseClient:::isDefined(datasources, day.name)
    }, error = function(e) {
      stop(paste0("Cannot access '", day.name, "'. The object or column does not exist on the server. Please verify the object/column name is correct."), call. = FALSE)
    })
  }
  if(!is.null(hour.name)){
    tryCatch({
      dsBaseClient:::isDefined(datasources, hour.name)
    }, error = function(e) {
      stop(paste0("Cannot access '", hour.name, "'. The object or column does not exist on the server. Please verify the object/column name is correct."), call. = FALSE)
    })
  }
  if(!is.null(minute.name)){
    tryCatch({
      dsBaseClient:::isDefined(datasources, minute.name)
    }, error = function(e) {
      stop(paste0("Cannot access '", minute.name, "'. The object or column does not exist on the server. Please verify the object/column name is correct."), call. = FALSE)
    })
  }
  if(!is.null(second.name)){
    tryCatch({
      dsBaseClient:::isDefined(datasources, second.name)
    }, error = function(e) {
      stop(paste0("Cannot access '", second.name, "'. The object or column does not exist on the server. Please verify the object/column name is correct."), call. = FALSE)
    })
  }

  if(is.null(newobj)){
    newobj <- "makeDate.newobj"
  }

  # Call server-side function
  calltext <- call("makeDateDS", year.name, month.name, day.name, hour.name, minute.name, second.name, format, newobj)
  tryCatch({
    DSI::datashield.assign(datasources, newobj, calltext)
  }, error = function(e) {
    # Check if error is about non-existent columns/objects
    error_msg <- as.character(e$message)
    if(grepl("Cannot access", error_msg) || grepl("does not exist", error_msg, ignore.case = TRUE)){
      stop(error_msg, call. = FALSE)
    } else {
      # Re-throw other errors with context
      stop(paste0("Error creating date from columns: ", error_msg), call. = FALSE)
    }
  })

  # Handle add_as_column logic
  if(add_as_column){
    # Determine table name
    target_table <- table.name
    
    if(is.null(target_table)){
      # Try to infer from year.name if it contains "$"
      if(grepl("[$]", year.name)){
        target_table <- stringr::str_extract_all(year.name, "^\\w+")[[1]]
      } else {
        stop("Cannot determine table name. Please provide [table.name] parameter when [add_as_column=TRUE] and inputs are not from a table column.", call.=FALSE)
      }
    }
    
    # Add column to table using dsDates cbind function (requires name parameter)
    DSI::datashield.assign.expr(datasources,
                                target_table,
                                paste0("cbind(", target_table, ", ", newobj, ", name = '", newobj, "')"))
    # Clean up temporary object
    invisible(ds.rm(newobj, datasources = datasources))
  }
}

