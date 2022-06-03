#' @title Subsets data frames in the server side
#'
#' @description Subsets a dataframe by concrete dates or ranges.
#'
#' @details The \code{day}, \code{month} and \code{year} arguments can be combined (e.g. to subset the year 2022 month 1).
#'
#' @param day a numeric value to specify a day to subset
#' @param month a numeric value to specify a month to subset
#' @param year a numeric value to specify a year to subset
#' @param range a character vector (of length 2) to provide a start and end date to perform the subset. Use the format \code{"yyyy-mm-dd"}.
#' @param newobj a character string that provides the name for the output object that is stored on the data servers.
#' Default \code{"dateSubset"}.
#' @template datasources
#' @param x.name a character string providing the name of the dataframe to be subsetted.
#' @param date_column a character specifying the column of \code{x.name} of type Date to be used for the subset computation.
#'
#' @return Does not have a return object.
#' @export

ds.subsetByDate <- function(x.name, date_column, day = NULL, month = NULL, year = NULL, range = NULL, newobj = NULL, datasources = NULL){

  if(is.null(datasources)){
    datasources <- DSI::datashield.connections_find()
  }

  dsBaseClient:::isDefined(datasources, x.name)

  if(!all(dsBaseClient:::checkClass(datasources = datasources, obj = paste0(x.name, "$", date_column)) == "Date")){
    stop("Selected [date_column] is not of class `Date`. See function `ds.asDate()`")
  }

  if(!is.null(range) & any(c(!is.null(day), !is.null(month), !is.null(year)))){
    warning("A [range] has been supplied alongside a [day/month/year]. Only the [range] will be used to subset the dataframe")
    day <- month <- year <- NULL
  } else if (!is.null(range)) {
    if(length(range) != 2){
      stop("[range] has to be a string vector of length 2: start/end")
    }
    conversions <- lapply(range, function(x){
      lubridate::as_date(x, format = "%Y-%m-%d")
    })
    if(any(is.na(unlist(conversions)))){
      stop("[range] has to be formatted as yyyy-mm-dd")
    }
  } else if (all(c(is.null(day), is.null(month), is.null(year), is.null(range)))) {
    stop("Input a [day/month/year] or [range]")
  }

  if(is.null(newobj)){
    newobj <- "dateSubset"
  }

  times <- ds.dim(x.name, datasources = datasources)
  lapply(1:(length(times)-1), function(x){
    ds.rep(x1 = 1,
           times = times[[x]][1],
           source.times = "c",
           source.each = "c",
           newobj = "subsetByDate_comparator1",
           datasources = datasources[x])
  })

  calltext <- call("subsetByDateDS", x.name, date_column, day, month, year, range)
  DSI::datashield.assign(datasources, "subsetByDate_comparator2", calltext)

  dsBaseClient::ds.dataFrameSubset(df.name = x.name,
                                   V1.name = "subsetByDate_comparator1",
                                   V2.name = "subsetByDate_comparator2",
                                   Boolean.operator = "==",
                                   newobj = newobj,
                                   datasources = datasources)
}
