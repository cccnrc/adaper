#' Find ocurrences of a specific value in multiple columns
#'
#' This function allows to specify a value that must be identified on multiple columns
#' and it returns a 0/1 vector based on absence/presence of the value
#'
#' @param data_input: the dataframe input object
#' @param value: the value to check for
#' @param colname_vector: vector of column names to check
#' @return A vector() object with 0/1 based on absence/presence of specified value in specifed columns for each sample
#' @export
value_multiple_columns <- function( data_input, value, colname_vector )
{
  res_vector <- base::vector()
  for ( i in 1:base::nrow(data_input) )
  {
    data_subset <- data_input[ i, colname_vector ]
    res <- 0
    if ( base::any( value %in% data_subset ) ) {
      res <- 1
    }
    res_vector <- c( res_vector, res )
  }
  return( res_vector )
}
