#' @title Computes diference in days between two date objects
#'
#' @description Computes the time difference using the lubridate package. Both input objects have to be created with \link{ds.asDate}.
#'
#' @details The time difference is computed as an absolute value.
#'
#' @param time1 a character string providing the name of a Date object on the server
#' @param time2 a character string providing the name of a Date object on the server
#' @param newobj a character string that provides the name for the output object that is stored on the data servers.
#' Default \code{"dateDiff"}.
#' @template add_as_column
#' @template datasources
#'
#' @return Does not have a return object.
#' @export

ds.dateDiff <- function(time1, time2, newobj = NULL, add_as_column = TRUE, datasources = NULL){

  if(is.null(datasources)){
    datasources <- DSI::datashield.connections_find()
  }

  if(is.null(newobj)){
    newobj <- "dateDiff"
  }

  if(!all(c(dsBaseClient:::checkClass(datasources = datasources, obj = time1),
            dsBaseClient:::checkClass(datasources = datasources, obj = time2)) == "Date")){
    stop("Selected [date_column] is not of class `Date`. See function `ds.asDate()`")
  }

  dsBaseClient::ds.make(toAssign =  paste(time1, "-", time2), newobj = newobj, datasources = datasources)
  ds.asNumeric(x.name = newobj, newobj = newobj, datasources = datasources)
  ds.abs(x = newobj, newobj = newobj, datasources = datasources)

  if(grepl("[$]", time1) & grepl("[$]", time2) & length(unique(unlist(stringr::str_extract_all(c(time1, time2), "^\\w+")))) & add_as_column){
    ds.cbind(x = c(stringr::str_extract_all(time1, "^\\w+")[[1]], newobj),
             newobj = stringr::str_extract_all(time1, "^\\w+")[[1]],
             datasources = datasources)
    invisible(ds.rm(newobj, datasources = datasources))
  }

}
