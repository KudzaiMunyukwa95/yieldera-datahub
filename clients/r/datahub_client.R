# Yieldera DataHub R Client
#
# Simple wrapper for accessing DataHub climate data API

library(httr)
library(jsonlite)
library(dplyr)

#' Initialize DataHub Client
#'
#' @param base_url API base URL
#' @param api_key Optional API key for authentication
#' @return Environment with client functions
#' @export
datahub_client <- function(base_url, api_key = NULL) {
  base_url <- sub("/$", "", base_url)
  
  headers <- list()
  if (!is.null(api_key)) {
    headers <- add_headers(Authorization = paste("Bearer", api_key))
  }
  
  list(
    base_url = base_url,
    headers = headers,
    
    # Get CHIRPS rainfall timeseries for a point
    get_chirps_timeseries = function(lat, lon, start_date, end_date, spatial_stat = "mean") {
      payload <- list(
        geometry = list(
          type = "point",
          lat = lat,
          lon = lon
        ),
        date_range = list(
          start = start_date,
          end = end_date
        ),
        spatial_stat = spatial_stat
      )
      
      response <- POST(
        paste0(base_url, "/api/data/chirps/timeseries"),
        body = payload,
        encode = "json",
        headers
      )
      
      stop_for_status(response)
      
      data <- content(response, "parsed")
      df <- do.call(rbind, lapply(data$data, as.data.frame))
      df$date <- as.Date(df$date)
      
      return(df)
    },
    
    # Get CHIRPS rainfall for a polygon
    get_chirps_polygon = function(wkt, start_date, end_date, spatial_stat = "mean") {
      payload <- list(
        geometry = list(
          type = "wkt",
          wkt = wkt
        ),
        date_range = list(
          start = start_date,
          end = end_date
        ),
        spatial_stat = spatial_stat
      )
      
      response <- POST(
        paste0(base_url, "/api/data/chirps/timeseries"),
        body = payload,
        encode = "json",
        headers
      )
      
      stop_for_status(response)
      
      data <- content(response, "parsed")
      df <- do.call(rbind, lapply(data$data, as.data.frame))
      df$date <- as.Date(df$date)
      
      return(df)
    },
    
    # Get ERA5-Land temperature timeseries
    get_era5land_timeseries = function(lat, lon, start_date, end_date, spatial_stat = "mean") {
      payload <- list(
        geometry = list(
          type = "point",
          lat = lat,
          lon = lon
        ),
        date_range = list(
          start = start_date,
          end = end_date
        ),
        spatial_stat = spatial_stat
      )
      
      response <- POST(
        paste0(base_url, "/api/data/era5land/timeseries"),
        body = payload,
        encode = "json",
        headers
      )
      
      stop_for_status(response)
      
      data <- content(response, "parsed")
      df <- do.call(rbind, lapply(data$data, as.data.frame))
      df$date <- as.Date(df$date)
      
      return(df)
    },
    
    # Export CHIRPS as GeoTIFF
    export_chirps_geotiff = function(wkt, start_date, end_date, 
                                    resolution_deg = 0.05, 
                                    tiff_mode = "multiband") {
      payload <- list(
        geometry = list(
          type = "wkt",
          wkt = wkt
        ),
        date_range = list(
          start = start_date,
          end = end_date
        ),
        resolution_deg = resolution_deg,
        tiff_mode = tiff_mode
      )
      
      response <- POST(
        paste0(base_url, "/api/data/chirps/geotiff"),
        body = payload,
        encode = "json",
        headers
      )
      
      stop_for_status(response)
      
      data <- content(response, "parsed")
      return(data$job_id)
    },
    
    # Get job status
    get_job_status = function(job_id) {
      response <- GET(
        paste0(base_url, "/api/data/jobs/", job_id, "/status"),
        headers
      )
      
      stop_for_status(response)
      
      return(content(response, "parsed"))
    },
    
    # Wait for job to complete
    wait_for_job = function(job_id, timeout = 300, poll_interval = 5) {
      start_time <- Sys.time()
      
      while (as.numeric(difftime(Sys.time(), start_time, units = "secs")) < timeout) {
        status <- get_job_status(job_id)
        
        if (status$status == "done") {
          return(status)
        } else if (status$status == "error") {
          stop(paste("Job failed:", status$error))
        }
        
        Sys.sleep(poll_interval)
      }
      
      stop(paste("Job", job_id, "did not complete within", timeout, "seconds"))
    },
    
    # Download GeoTIFF
    download_geotiff = function(job_id, output_path = NULL) {
      job_data <- wait_for_job(job_id)
      download_url <- job_data$download_urls$tif
      
      if (!is.null(output_path)) {
        download.file(download_url, output_path, mode = "wb")
        return(output_path)
      } else {
        return(download_url)
      }
    },
    
    # Health check
    health_check = function() {
      response <- GET(
        paste0(base_url, "/api/data/health"),
        headers
      )
      
      stop_for_status(response)
      return(content(response, "parsed"))
    },
    
    # List datasets
    list_datasets = function() {
      response <- GET(
        paste0(base_url, "/api/data/datasets"),
        headers
      )
      
      stop_for_status(response)
      return(content(response, "parsed"))
    }
  )
}


# Example usage
if (interactive()) {
  # Initialize client
  client <- datahub_client(
    base_url = "https://api.yieldera.co.zw",
    api_key = "your_api_key_here"
  )
  
  # Health check
  health <- client$health_check()
  print(paste("API Status:", health$status))
  
  # Get CHIRPS rainfall for Harare
  df_rain <- client$get_chirps_timeseries(
    lat = -17.8249,
    lon = 31.0530,
    start_date = "2024-10-01",
    end_date = "2024-12-31"
  )
  print(paste("Rainfall data:", nrow(df_rain), "days"))
  print(head(df_rain))
  
  # Plot rainfall
  plot(df_rain$date, df_rain$precip_mm, 
       type = "l",
       xlab = "Date", 
       ylab = "Rainfall (mm/day)",
       main = "CHIRPS Daily Rainfall - Harare")
  
  # Get temperature
  df_temp <- client$get_era5land_timeseries(
    lat = -17.8249,
    lon = 31.0530,
    start_date = "2024-01-01",
    end_date = "2024-01-31"
  )
  print(paste("Temperature data:", nrow(df_temp), "days"))
  print(head(df_temp))
  
  # Plot temperature range
  plot(df_temp$date, df_temp$tmax_c,
       type = "l", col = "red",
       xlab = "Date",
       ylab = "Temperature (°C)",
       main = "ERA5-Land Temperature - Harare",
       ylim = range(c(df_temp$tmin_c, df_temp$tmax_c)))
  lines(df_temp$date, df_temp$tmin_c, col = "blue")
  lines(df_temp$date, df_temp$tavg_c, col = "black", lty = 2)
  legend("topright", 
         legend = c("Tmax", "Tavg", "Tmin"),
         col = c("red", "black", "blue"),
         lty = c(1, 2, 1))
  
  # Export GeoTIFF
  polygon_wkt <- "POLYGON((31.0 -17.9, 31.2 -17.9, 31.2 -17.7, 31.0 -17.7, 31.0 -17.9))"
  job_id <- client$export_chirps_geotiff(
    wkt = polygon_wkt,
    start_date = "2024-01-01",
    end_date = "2024-01-31"
  )
  print(paste("GeoTIFF export job created:", job_id))
  
  # Download
  download_url <- client$download_geotiff(job_id)
  print(paste("Download URL:", download_url))
}
