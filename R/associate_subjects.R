#' Find ocurrences in dataframe2 based on equal and tolerance values in dataframe1
#'
#' This function allows to specify column that must have identical values and columns
#' that must have similar values (with specifide tolerance) between two databases and
#' it will screen to identify subjects based on passed parameters. IT will return a
#' dataframe which is the merge of both databases for identified subjects, and a dataframe
#' which is represented by unidentifed subjects in first dataframe.
#'
#' @param data_input1: the first dataframe object to extract subjects to identify
#' @param data_input2: the second dataframe object to check for subject identities
#' @param equal_colname_vector: vector of column names for variables that must be identical between the dataframes
#' @param tolerance_colname_vector: vector of column names for variables that must be similar between the dataframes
#' @param tolerance_value_vector: vector of tolerance values (must be same length of "tolerance_colname_vector")
#' @return A list() object with "$data": the merged dataframes for identified subjects; and "$unidentified_data": the subset of dataframe for unidentified subjects (both for "data_input1" and "data_input2")
#' @export
associate_subjects <- function(
  data_input1,
  data_input2,
  equal_colname_vector = NULL,
  tolerance_colname_vector = NULL,
  tolerance_value_vector = NULL
)
{
  ### check passed parameters
  if ( (base::is.null( equal_colname_vector )) & (base::is.null( tolerance_colname_vector )) ) {
    base::stop( ' "equal_colname_vector" or "tolerance_colname_vector" must be passed to the function \n', sep = '' )
  } else {
    if ( ! base::is.null(tolerance_colname_vector) ) {
      if ( base::is.null(tolerance_value_vector) ) {
        base::stop( ' both "tolerance_value_vector" and "tolerance_colname_vector" must be passed \n', sep = '' )
      } else {
        if ( base::length( tolerance_colname_vector ) != base::length( tolerance_value_vector ) ) {
          base::stop( ' "tolerance_value_vector" and "tolerance_colname_vector" must have same length \n', sep = '' )
        }
      }
    }
  }
  ### check starting numbers
  data_input1_num <- base::nrow( data_input1 )
  data_input2_num <- base::nrow( data_input2 )
  cat( ' - passed dataframe, data_input1: ', data_input1_num, '\n', sep = ''  )
  cat( ' - passed dataframe, data_input2: ', data_input2_num, '\n', sep = ''  )
  ### create a fake ID column to check for duplicates
  data_input1[['fake.ID1']] <- base::as.numeric( base::rownames( data_input1 ) )
  data_input2[['fake.ID2']] <- base::as.numeric( base::rownames( data_input2 ) )
  ### check all colnames are in passed dataframes
  all_colname_vector <- c(equal_colname_vector, tolerance_colname_vector)
  for (i in 1:base::length( all_colname_vector ))
  {
    colname_i <- all_colname_vector[i]
    if (( ! colname_i %in% base::colnames(data_input1) ) | ( ! colname_i %in% base::colnames(data_input1) )) {
      base::stop( ' column: ', colname_i, ' not found in input data \n', sep = '' )
    }
  }
  ### create inner_join of equal dataframes
  if ( ! base::is.null( equal_colname_vector ) ) {
    if ( base::is.null( tolerance_colname_vector ) ) {
      equal_join <- dplyr::inner_join( data_input1, data_input2, by=equal_colname_vector )
      equal_join_num <- base::nrow( equal_join )
      equal_join_dup1_num <- base::length( base::which( base::duplicated( equal_join[['fake.ID1']] ) ))
      equal_join_dup2_num <- base::length( base::which( base::duplicated( equal_join[['fake.ID2']] ) ))
      cat( '   - after equal columns inner_join, cases identified: ', equal_join_num, '\n', sep = '' )
      cat( '   - after equal columns inner_join, data_input1 duplicated cases: ', equal_join_dup1_num, '\n', sep = '' )
      cat( '   - after equal columns inner_join, data_input2 duplicated cases: ', equal_join_dup2_num, '\n', sep = '' )
      ### select cases not identified in data_input1
      equal_join_missing1 <- data_input1[ ! data_input1[['fake.ID1']] %in% equal_join[['fake.ID1']], ]
      equal_join_missing1_num <- base::nrow( equal_join_missing1 )
      cat( '   - after equal columns inner_join, data_input1 unidentified cases: ', equal_join_missing1_num, '\n', sep = '' )
    } else {
      index1_vector <- base::vector()
      index2_vector <- base::vector()
      index1_unidentified_vector <- base::vector()
      index1_equal_vector <- base::vector()
      index1_tolerance_vector <- base::vector()
      index1_multiple_vector <- base::vector()
      for ( i in 1:base::nrow(data_input1) )
      {
        index_i <- data_input1[['fake.ID1']][i]
        equal_string <- base::paste( '',  equal_colname_vector[1],  ' == ', data_input1[i,equal_colname_vector[1]], sep = '' )
        for ( r in 2:base::length(equal_colname_vector) )
        {
          colname_r <- equal_colname_vector[r]
          equal_string <- base::paste( equal_string,  ' & ', '',  colname_r,  ' == ', data_input1[i,equal_colname_vector[r]], sep = '' )
        }
        subset_equal_r <- base::subset( data_input2, base::eval( base::parse( text = equal_string ) ) )
        subset_equal_r_num <- base::nrow(subset_equal_r)
        # cat( '    - matching cases: ', base::nrow(subset_equal_r), '\n', sep = '' )
        ### if more than one cases macthes use ranges
        if ( subset_equal_r_num > 1 ) {
          # cat( '    - matching cases: ', base::nrow(subset_equal_r), '\n', sep = '' )
          tolerance_string <- base::paste( '',  tolerance_colname_vector[1],  ' >= ', data_input1[i,tolerance_colname_vector[1]] - tolerance_value_vector[1], ' & ',  tolerance_colname_vector[1],  ' <= ', data_input1[i,tolerance_colname_vector[1]] + tolerance_value_vector[1], sep = '' )
          if ( base::length( tolerance_colname_vector ) > 1 ) {
            for ( x in 2:base::length( tolerance_colname_vector ) )
            {
              tolerance_string <- base::paste( tolerance_string, ' & ',  tolerance_colname_vector[x],  ' >= ', data_input1[i,tolerance_colname_vector[x]] - tolerance_value_vector[x], ' & ',  tolerance_colname_vector[x],  ' <= ', data_input1[i,tolerance_colname_vector[x]] + tolerance_value_vector[x], sep = '' )
            }
          }
          # cat( tolerance_string, '\n', sep = '' )
          subset_tolerance <- base::subset( subset_equal_r, base::eval( base::parse( text = tolerance_string ) ) )
          subset_tolerance_num <- base::nrow(subset_tolerance)
          if ( subset_tolerance_num == 1 ) {
            index1_vector <- c( index1_vector, index_i )
            index1_tolerance_vector <- c( index1_tolerance_vector, index_i )
            index2_vector <- c( index2_vector, subset_tolerance[['fake.ID2']] )
          } else if ( subset_tolerance_num == 0 ) {
            index1_unidentified_vector <- c( index1_unidentified_vector, index_i )
          } else {
            index1_multiple_vector <- c( index1_multiple_vector, index_i )
          }
          # cat( '    - tolerance cases: ', base::nrow(subset_equal_r), '\n', sep = '' )
        } else if ( subset_equal_r_num == 0 ) {
          index1_unidentified_vector <- c( index1_unidentified_vector, index_i )
          # cat( '    - ', i, ': NO matching case \n', sep = '' )
        } else if ( subset_equal_r_num == 1 ) {
          # cat( '    - ', i, ': single matching case \n', sep = '' )
          index1_vector <- c( index1_vector, index_i )
          index2_vector <- c( index2_vector, subset_equal_r[['fake.ID2']] )
          index1_equal_vector <- c( index1_equal_vector, index_i )
        }
      }
      index1_vector_num <- base::length( index1_vector )
      index2_vector_num <- base::length( index2_vector )
      index1_vector_equal_num <- base::length( index1_equal_vector )
      index1_vector_tolerance_num <- base::length( index1_tolerance_vector )
      index1_multiple_vector_num <- base::length( index1_multiple_vector )
      index1_vector_equal_perc <- base::round( index1_vector_equal_num / index1_vector_num *100, 2 )
      index1_vector_tolerance_perc <- base::round( index1_vector_tolerance_num / index1_vector_num *100, 2 )
      index1_multiple_vector_perc <- base::round( index1_multiple_vector_num / data_input1_num * 100, 2 )
      index1_unidentified_vector_num <- base::length( index1_unidentified_vector )
      index1_vector_duplicated <- base::length( base::which( base::duplicated( index1_vector ) ) )
      index2_vector_duplicated <- base::length( base::which( base::duplicated( index2_vector ) ) )
      index1_vector_perc <- base::round( index1_vector_num / data_input1_num * 100, 2 )
      index2_vector_perc <- base::round( index2_vector_num / data_input2_num * 100, 2 )
      index1_vector_duplicated_perc <- base::round( index1_vector_duplicated / index1_vector_num * 100, 2 )
      index2_vector_duplicated_perc <- base::round( index2_vector_duplicated / index2_vector_num * 100, 2 )
      index1_unidentified_vector_perc <- base::round( index1_unidentified_vector_num / data_input1_num * 100, 2 )
      cat( ' - after tolerance filtering identified: ', index1_vector_num, ' cases in dataframe1 (', index1_vector_perc, '%)  -> $data', '\n', sep = '' )
      cat( ' - after tolerance filtering identified: ', index2_vector_num, ' cases in dataframe2 (', index2_vector_perc, '%)', '\n', sep = '' )
      cat( ' - after tolerance filtering identified: ', index1_vector_equal_num, ' as identical matches (', index1_vector_equal_perc, '%)', '\n', sep = '' )
      cat( ' - after tolerance filtering identified: ', index1_vector_tolerance_num, ' as tolerance matches (', index1_vector_tolerance_perc, '%)', '\n', sep = '' )
      cat( ' - after tolerance filtering unidentified: ', index1_multiple_vector_num, ' as multiple matches (', index1_multiple_vector_perc, '%)', '\n', sep = '' )
      cat( ' - after tolerance filtering duplicated: ', index1_vector_duplicated, ' duplicated in dataframe 1 (', index1_vector_duplicated_perc,'%)', '\n', sep = '' )
      cat( ' - after tolerance filtering duplicated: ', index2_vector_duplicated, ' duplicated in dataframe 2 (', index2_vector_duplicated_perc,'%)', '\n', sep = '' )
      cat( ' - after tolerance filtering unidentified: ', index1_unidentified_vector_num, ' unidentified cases in dataframe 1 (', index1_unidentified_vector_perc,'%) -> $unidentified_data1', '\n', sep = '' )
      index2_unidentified_vector <- data_input2[['fake.ID2']][ -index2_vector ]
      index2_unidentified_vector_num <- base::length( index2_unidentified_vector )
      index2_unidentified_vector_perc <- base::round( index2_unidentified_vector_num / base::nrow(data_input2) * 100, 2 )
      cat( ' - after tolerance filtering unidentified: ', index2_unidentified_vector_num, ' unidentified cases in dataframe 1 (', index2_unidentified_vector_perc, '%) -> $unidentified_data2', '\n', sep = '' )
      ### recreate identifed merged dataframe
      out_df <- base::cbind( data1 = data_input1[ index1_vector, ], data2 = data_input2[ index2_vector, ] )
      out_list <- base::list(
        'data' = out_df,
        'unidentified_data1' = data_input1[ index1_unidentified_vector, ],
        'unidentified_data2' = data_input2[ index2_unidentified_vector, ]
      )
    }
  }
  return(out_list)
}
