#' @title Computes difference between two date objects
#'
#' @description Computes the time difference between two Date objects. Both input objects have to be created with \link{ds.asDate} or \link{ds.makeDate}.
#' The difference can be computed in days (default), months, or years.
#'
#' @details The time difference is computed as an absolute value. When using months or years, the result is calculated by dividing 
#' the difference in days by the average number of days per month (30.44) or per year (365.25), respectively.
#'
#' @param time1 a character string providing the name of a Date object on the server
#' @param time2 a character string providing the name of a Date object on the server
#' @param unit a character string specifying the unit for the difference: "days" (default), "months", or "years"
#' @param as_integer a logical value indicating whether to truncate the result to integer values (remove decimals).
#' Default is \code{FALSE} (keeps decimal values).
#' @param newobj a character string that provides the name for the output object that is stored on the data servers.
#' Default \code{"dateDiff"}.
#' @template add_as_column
#' @template datasources
#'
#' @return Does not have a return object.
#'
#' @examples
#' \dontrun{
#' # Difference in days (default)
#' ds.dateDiff(time1 = "birth_date", time2 = "visit_date", unit = "days")
#'
#' # Difference in months
#' ds.dateDiff(time1 = "birth_date", time2 = "visit_date", unit = "months")
#'
#' # Difference in years
#' ds.dateDiff(time1 = "birth_date", time2 = "visit_date", unit = "years")
#'
#' # Difference in years as integer (truncated)
#' ds.dateDiff(time1 = "birth_date", time2 = "visit_date", unit = "years", as_integer = TRUE)
#' }
#'
#' @export

ds.dateDiff <- function(time1, time2, unit = "days", as_integer = FALSE, newobj = NULL, add_as_column = TRUE, datasources = NULL){

  if(is.null(datasources)){
    datasources <- DSI::datashield.connections_find()
  }

  if(is.null(newobj)){
    newobj <- "dateDiff"
  }

  # Validate unit parameter
  if(!unit %in% c("days", "months", "years")){
    stop("[unit] must be one of: 'days', 'months', or 'years'", call. = FALSE)
  }

  if(!all(c(dsBaseClient:::checkClass(datasources = datasources, obj = time1),
            dsBaseClient:::checkClass(datasources = datasources, obj = time2)) == "Date")){
    stop("Selected [date_column] is not of class `Date`. See function `ds.asDate()` or `ds.makeDate()`")
  }

  # Calculate difference in days first
  dsBaseClient::ds.make(toAssign =  paste(time1, "-", time2), newobj = newobj, datasources = datasources)
  ds.asNumeric(x.name = newobj, newobj = newobj, datasources = datasources)
  ds.abs(x = newobj, newobj = newobj, datasources = datasources)
  
  # Convert to requested unit if not days
  if(unit == "months"){
    # Divide by average days per month (30.44)
    dsBaseClient::ds.make(toAssign = paste(newobj, "/", 30.44), newobj = newobj, datasources = datasources)
  } else if(unit == "years"){
    # Divide by average days per year (365.25, accounting for leap years)
    dsBaseClient::ds.make(toAssign = paste(newobj, "/", 365.25), newobj = newobj, datasources = datasources)
  }
  # If unit == "days", no conversion needed
  
  # Truncate to integer if requested
  if(as_integer){
    dsBaseClient::ds.make(toAssign = paste("as.integer(", newobj, ")"), newobj = newobj, datasources = datasources)
  }

  if(grepl("[$]", time1) & grepl("[$]", time2) & length(unique(unlist(stringr::str_extract_all(c(time1, time2), "^\\w+")))) & add_as_column){
    ds.cbind(x = c(stringr::str_extract_all(time1, "^\\w+")[[1]], newobj),
             newobj = stringr::str_extract_all(time1, "^\\w+")[[1]],
             datasources = datasources)
    invisible(ds.rm(newobj, datasources = datasources))
  }

}
