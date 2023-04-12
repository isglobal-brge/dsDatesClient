#' @title Converts a server-side R object into a Date class
#'
#' @description Converts the input object into a date class. This function is based on the lubridate function \code{as_date}
#'
#' @param x.name a character string providing the name of the input object to be coerced to date character.
#' @param newobj a character string that provides the name for the output object that is stored on the data servers.
#' Default \code{"asDate.newobj"}.
#' @template datasources
#' @param format A character string. The default for the format methods is "%Y-%m-%d".
#' Check the details section of \link{strptime} for further information.
#' @param origin something which can be coerced by \code{as_Date(origin)} to such an object (default: the Unix epoch of "1970-01-01").
#' Note that in this instance, the server values of \code{x.name} is assumed to reflect the number of days since \code{origin}.
#' @template add_as_column
#'
#' @return Does not have a return object.
#' @export

ds.asDate <- function(x.name, format = NULL, origin = NULL, newobj = NULL, add_as_column = TRUE, datasources = NULL){
  # TODO pensar que fer amb el format, no es poden passar caracters especials! format = "%Y-%m-%d"
  if (is.null(datasources)) {
    datasources <- DSI::datashield.connections_find()
  }

  if(!(is.list(datasources) && all(unlist(lapply(datasources, function(d) {methods::is(d,"DSConnection")}))))){
    stop("The 'datasources' were expected to be a list of DSConnection-class objects", call.=FALSE)
  }

  if(!is.null(origin) & !inherits(origin, "character")){
    stop("[origin] must be a character string")
  }

  dsBaseClient:::isDefined(datasources, x.name)

  if(is.null(newobj)){
    newobj <- "asDate.newobj"
  }
  
  if(!is.null(format)){
    format <- sf::rawToHex(serialize(format,  NULL))
  }

  calltext <- call("asDateDS", x.name, format, origin)
  DSI::datashield.assign(datasources, newobj, calltext)

  if(grepl("[$]", x.name) & add_as_column){
    DSI::datashield.assign.expr(datasources,
                                stringr::str_extract_all(x.name, "^\\w+")[[1]],
                                paste0("cbind(", stringr::str_extract_all(x.name, "^\\w+")[[1]], ", ", newobj, ")"))
    # ds.cbind(x = c(stringr::str_extract_all(x.name, "^\\w+")[[1]], newobj),
    #          newobj = stringr::str_extract_all(x.name, "^\\w+")[[1]],
    #          datasources = datasources)
    invisible(ds.rm(newobj, datasources = datasources))
  }

}
