library(rsyncrosim)
suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(terra))

# Setup ----
progressBar(type = "message", message = "Preparing inputs...")

# Initialize first breakpoint for timing code
currentBreakPoint <- proc.time()

## Check Prometheus installation ----
if (.Platform$OS.type == "unix") {
  stop("Prometheus is currently not supported on Unix systems.")
}

prometheusLocation <- Sys.which("prometheus.exe")
if (prometheusLocation == "") {
  stop("Could not find the Prometheus installation location. Please check that Prometheus is installed correctly.")
}

prometheusVersion <- str_c('powershell "(Get-Item -path ', prometheusLocation, ').VersionInfo.ProductVersion"') %>%
  shell(intern = T)
if (prometheusVersion != "6,2021,12,03") {
  stop("Could not find the correct version of Prometheus. Please ensure that you have installed Prometheus v2021.12.03.")
}

## Connect to SyncroSim ----

myScenario <- scenario()

# Load run controls and get iterations
RunControl <- datasheet(myScenario, "burnP3Plus_RunControl")
iterations <- seq(RunControl$MinimumIteration, RunControl$MaximumIteration)

# Load remaining datasheets
ResampleOption <- datasheet(myScenario, "burnP3Plus_FireResampleOption")
DeterministicIgnitionCount <- datasheet(myScenario, "burnP3Plus_DeterministicIgnitionCount", lookupsAsFactors = F, optional = T) %>%
  unique() %>%
  filter(Iteration %in% iterations) %>%
  pull(Ignitions, Iteration)
DeterministicIgnitionLocation <- datasheet(myScenario, "burnP3Plus_DeterministicIgnitionLocation", lookupsAsFactors = F, optional = T) %>%
  unique() %>%
  filter(Iteration %in% iterations)
DeterministicBurnCondition <- datasheet(myScenario, "burnP3Plus_DeterministicBurnCondition", lookupsAsFactors = F, optional = T) %>%
  unique() %>%
  filter(Iteration %in% iterations)
FuelType <- datasheet(myScenario, "burnP3Plus_FuelType")
FuelTypeCrosswalk <- datasheet(myScenario, "burnP3PlusPrometheus_FuelCodeCrosswalk", lookupsAsFactors = F, optional = T)
ValidFuelCodes <- datasheet(myScenario, "burnP3PlusPrometheus_FuelCode") %>% pull()
WindGrid <- datasheet(myScenario, "burnP3Plus_WindGrid", lookupsAsFactors = F, optional = T)
GreenUp <- datasheet(myScenario, "burnP3Plus_GreenUp", lookupsAsFactors = F, optional = T)
Curing <- datasheet(myScenario, "burnP3Plus_Curing", lookupsAsFactors = F, optional = T)
FuelLoad <- datasheet(myScenario, "burnP3Plus_FuelLoad", lookupsAsFactors = F, optional = T)
OutputOptions <- datasheet(myScenario, "burnP3Plus_OutputOption", optional = T)
OutputOptionsSpatial <- datasheet(myScenario, "burnP3Plus_OutputOptionSpatial", optional = T)
OutputOptionsSpatialPrometheus <- datasheet(myScenario, "burnP3PlusPrometheus_OutputOptionSpatial", optional = T)

# Import relevant rasters
# - Note that datasheetRaster is avoided as it requires rgdal
# - Under conda, this causes Prometheus to point to the wrong version of GDAL
fuelsRaster <- datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["FuelGridFileName"]] %>% rast()
elevationRaster <- tryCatch(
  datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["ElevationGridFileName"]] %>% rast(),
  error = function(e) NULL
)

## Handle empty values ----
if (nrow(FuelTypeCrosswalk) == 0) {
  updateRunLog("No fuels code crosswalk found! Using default crosswalk for Canadian Forest Service fuel codes.", type = "warning")
  FuelTypeCrosswalk <- read_csv(file.path(ssimEnvironment()$PackageDirectory, "Default Fuel Crosswalk.csv")) %>% as.data.frame()
  saveDatasheet(myScenario, FuelTypeCrosswalk, "burnP3PlusPrometheus_FuelCodeCrosswalk")
}

if (nrow(OutputOptions) == 0) {
  updateRunLog("No tabular output options chosen. Defaulting to keeping all tabular outputs.", type = "info")
  OutputOptions[1, ] <- rep(TRUE, length(OutputOptions[1, ]))
  saveDatasheet(myScenario, OutputOptions, "burnP3Plus_OutputOption")
}

if (nrow(OutputOptionsSpatial) == 0) {
  updateRunLog("No spatial output options chosen. Defaulting to keeping all spatial outputs.", type = "info")
  OutputOptionsSpatial[1, ] <- rep(TRUE, length(OutputOptionsSpatial[1, ]))
  saveDatasheet(myScenario, OutputOptionsSpatial, "burnP3Plus_OutputOptionSpatial")
}

if (nrow(OutputOptionsSpatialPrometheus) == 0) {
  updateRunLog("No Prometheus-specific spatial output options chosen. Defaulting to keeping no secondary spatial outputs.", type = "info")
  OutputOptionsSpatialPrometheus[1, ] <- rep(FALSE, length(OutputOptionsSpatialPrometheus[1, ]))
  saveDatasheet(myScenario, OutputOptionsSpatialPrometheus, "burnP3PlusPrometheus_OutputOptionSpatial")
}

if (nrow(ResampleOption) == 0) {
  ResampleOption[1, ] <- c(0, 0)
  saveDatasheet(myScenario, ResampleOption, "burnP3Plus_FireResampleOption")
}

if (nrow(GreenUp) == 0) {
  GreenUp[1, ] <- c(NA, TRUE)
  saveDatasheet(myScenario, GreenUp, "burnP3Plus_GreenUp")
} else if (is.character(GreenUp$GreenUp)) GreenUp$GreenUp <- GreenUp$GreenUp != "No"

if (nrow(Curing) == 0) {
  Curing[1, ] <- c(NA, 75L)
  saveDatasheet(myScenario, Curing, "burnP3Plus_Curing")
}

## Check raster inputs for consistency ----

# Ensure fuels crs can be converted to Lat / Long
if(fuelsRaster %>% is.lonlat){stop("Incorrect coordinate system. Projected coordinate system required, please reproject your grids.")}
tryCatch(fuelsRaster %>% project("EPSG:4326"), error = function(e) stop("Error parsing provided Fuels map. Cannot calculate Latitude and Longitude from provided Fuels map, please check CRS."))

# Define function to check input raster for consistency
checkSpatialInput <- function(x, name, checkProjection = T, warnOnly = F) {
  # Only check if not null
  if (!is.null(x)) {
    # Ensure comparable number of rows and cols in all spatial inputs
    if (nrow(fuelsRaster) != nrow(x) | ncol(fuelsRaster) != ncol(x)) {
      if (warnOnly) {
        updateRunLog("Number of rows and columns in ", name, " map do not match Fuels map. Please check that the extent and resolution of these maps match.", type = "warning")
        invisible(NULL)
      } else {
        stop("Number of rows and columns in ", name, " map do not match Fuels map. Please check that the extent and resolution of these maps match.")
      }
    }

    # Info if CRS is not matching
    if (checkProjection) {
      if (crs(x) != crs(fuelsRaster)) {
        updateRunLog("Projection of ", name, " map does not match Fuels map. Please check that the CRS of these maps match.", type = "info")
      }
    }
  }

  # Silently return for clean pipelining
  invisible(x)
}

# Check optional inputs
checkSpatialInput(elevationRaster, "Elevation")

## Set constants ----

# Names and codes of Prometheus-specific secondary outputs
outputComponentNames <- c("RateOfSpread", "FireIntensity", "SpreadDirection", "SurfaceFuelConsumption", "CrownFractionBurned", "CrownFractionConsumed", "TotalFuelConsumption")
outputComponentCodes <- c("ros", "fi", "raz", "sfc", "cfb", "cfc", "tfc")

## Extract relevant parameters ----

# Burn maps must be kept to generate summarized maps later, this boolean summarizes
# whether or not burn maps are needed
saveBurnMaps <- any(OutputOptionsSpatial$BurnMap, OutputOptionsSpatial$BurnProbability, OutputOptionsSpatial$BurnCount, any(OutputOptionsSpatialPrometheus))

minimumFireSize <- ResampleOption$MinimumFireSize

# Keep a counter of fires above the minimum fire size
fireCount <- 0

# Combine fuel type definitions with codes if provided
if (nrow(FuelTypeCrosswalk) > 0) {
  FuelType <- FuelType %>%
    left_join(FuelTypeCrosswalk, by = c("Name" = "FuelType"))
} else {
  FuelType <- FuelType %>%
    mutate(Code = Name)
}

# Decide whether or not to manually set grass fuel loading
useWindGrid <- nrow(WindGrid) > 0

# Decide whether or not to manually set grass fuel loading and curing
setFuelLoad <- nrow(FuelLoad) > 0
setGrassCuring <- nrow(Curing) > 0

## Error check fuels ----

# Ensure all fuel types are assigned to a fuel code
if (any(is.na(FuelType$Code))) {
  stop(
    "Could not find a valid Prometheus Fuel Code for one or more Fuel Types. Please add Prometheus Fuel Code Crosswalk records for the following Fuel Types in the project scope: ",
    FuelType %>% filter(is.na(Code)) %>% pull(Name) %>% str_c(collapse = "; ")
  )
}

# Ensure all fuel codes are valid
# - This should only occur if the Fuel Code Crosswalk is empty and Fuel Type names are being used as codes
if (any(!FuelType$Code %in% ValidFuelCodes)) {
  stop("Invalid fuel codes found in the Fuel Type definitions. Please consider setting an exlicit Fuel Code Crosswalk for Prometheus in the project scope.")
}

# Ensure that there are no fuels present in the grid that are not tied to a valid code
fuelIdsPresent <- unique(fuelsRaster) %>% pull()
if (any(!fuelIdsPresent %in% c(FuelType$ID, NaN))) {
  stop(
    "Found one or more values in the Fuels Map that are not assigned to a known Fuel Type. Please add definitions for the following Fuel IDs: ",
    setdiff(fuelIdsPresent, FuelType$ID) %>% str_c(collapse = " ")
  )
}

## Setup files and folders ----

# Create temp folder, ensure it is empty
tempDir <- ssimEnvironment()$TempDirectory %>%
  str_replace_all("\\\\", "/") %>%
  file.path("growth")
unlink(tempDir, recursive = T, force = T)
dir.create(tempDir, showWarnings = F)

# Set names for model input files to be created
fuelsRasterAscii <- file.path(tempDir, "fuels.asc")
fuelsRasterProjection <- file.path(tempDir, "fuels.prj")
fuelLookup <- file.path(tempDir, "fuels.lut")
weatherFile <- file.path(tempDir, "weather.txt")
parameterFile <- file.path(tempDir, "parameters.txt")

# Create folders for various outputs
gridOutputFolder <- file.path(tempDir, "grids")
shapeOutputFolder <- file.path(tempDir, "shapes")
accumulatorOutputFolder <- file.path(tempDir, "accumulator")
secondaryOutputFolder <- file.path(tempDir, "secondary")
allPerimOutputFolder <- file.path(tempDir, "all_perim")
dir.create(gridOutputFolder, showWarnings = F)
dir.create(shapeOutputFolder, showWarnings = F)
dir.create(accumulatorOutputFolder, showWarnings = F)
dir.create(secondaryOutputFolder, showWarnings = F)
dir.create(allPerimOutputFolder, showWarnings = F)

# Create placeholder rasters for potential outputs
burnAccumulator <- rast(fuelsRaster)

## Function Definitions ----

### Convenience and conversion functions ----

# Function to time code by returning a clean string of time since this function was last called
updateBreakpoint <- function() {
  # Calculate time since last breakpoint
  newBreakPoint <- proc.time()
  elapsed <- (newBreakPoint - currentBreakPoint)["elapsed"]

  # Update current breakpoint
  currentBreakPoint <<- newBreakPoint

  # Return cleaned elapsed time
  if (elapsed < 60) {
    return(str_c(round(elapsed), "sec"))
  } else if (elapsed < 60^2) {
    return(str_c(round(elapsed / 60, 1), "min"))
  } else {
    return(str_c(round(elapsed / 60 / 60, 1), "hr"))
  }
}

# Define a function to facilitate recoding values using a lookup table
lookup <- function(x, old, new) dplyr::recode(x, !!!set_names(new, old))

# Function to convert a raster and row and column indices to Lat Long
latlonFromRowCol <- function(x, row, col) {
  cellFromRowCol(x, row, col) %>%
    {
      as.points(x, na.rm = F)[.]
    } %>%
    project("EPSG:4326") %>%
    crds() %>%
    return()
}

# Function to convert from latlong to cell index
cellFromLatLong <- function(x, lat, long) {
  # Convert list of lat and long to SpatVector, reproject to source crs
  points <- matrix(c(long, lat), ncol = 2) %>%
    vect(crs = "EPSG:4326") %>%
    project(x)

  # Get vector of cell ID's from points
  return(cells(x, points)[, "cell"])
}

# Function to call Pandora on the (global) parameter file
runPandora <- function() {
  # Note than pandora can't handle spaces in the paramter file path
  # - if there are spaces in tempdir, copy the parameter file to a system temp file
  # - also update paramterFile location in the local scope
  if (str_detect(tempDir, " ")) {
    parameterTempFile <- tempfile(pattern = "pandora_parameter", fileext = ".txt")
    file.copy(parameterFile, parameterTempFile, overwrite = T)
    parameterFile <- parameterTempFile
  }

  ssimEnvironment()$PackageDirectory %>%
    str_replace_all("\\\\", "/") %>%
    str_c("/pandora.exe /silent /nowin ", parameterFile) %>%
    shell()
}

### File generation functions ----

# Function to convert daily weather data for every day of burning to format
# expected by Pandora and save to file
generateWeatherFile <- function(weatherData) {
  weatherData %>%
    # To convert daily weather to hourly, we need to repeat each row for every
    # hour burned that day and pad the rest of the day with zeros. To do this,
    # we first generate and append a row of all zeros.
    add_row() %>%
    mutate_all(function(x) c(head(x, -1), 0)) %>%
    # Next we use slice to repeat rows as needed.
    slice(pmap(.,
      function(BurnDay, HoursBurning, ..., zeroRowID) {
        if (BurnDay != 0) {
          c(rep(BurnDay, HoursBurning), rep(zeroRowID, 24 - HoursBurning))
        }
      },
      zeroRowID = nrow(.)
    ) %>%
      unlist()) %>%
    # Next we add in columns of mock date and time since this is requried by Pandora
    mutate(
      date = as.integer((row_number() + 12) / 24) + ymd(20000101),
      date = str_c(day(date), "/", month(date), "/", year(date)),
      time = (row_number() + 12) %% 24
    ) %>%
    # Finally we rename and reorder columns and write to file
    dplyr::select(HOURLY = date, HOUR = time, TEMP = Temperature, RH = RelativeHumidity, WD = WindDirection, WS = WindSpeed, PRECIP = Precipitation, HFFMC = FineFuelMoistureCode, HISI = InitialSpreadIndex, HFWI = FireWeatherIndex, DMC = DuffMoistureCode, DC = DroughtCode, BUI = BuildupIndex) %>%
    write_csv(weatherFile, escape = "none")
  invisible()
}

# Function to generate Pandora parameter file based on rows of the fireGrowthInputs dataframe
generateParameterFile <- function(Iteration, FireID, season, Lat, Lon, data) {
  # Define a unique identifier to name files
  fileTag <- str_c("it", Iteration, ".fid", FireID)

  # Build the parameter file line-by-line
  parameterFileText <- c(
    str_c("Fire_name ", fileTag),
    str_c("FBP_GridFile ", sources(fuelsRaster)),
    if (!is.null(elevationRaster)) {
      str_c("Elev_GridFile ", sources(elevationRaster))
    } else {
      NA
    },
    str_c("Fuel_Table ", fuelLookup),
    str_c("Ign_DateTime 1/1/2000:13:00:00"),
    str_c("Ign_Lon ", Lon),
    str_c("Ign_Lat ", Lat),
    str_c("WxStation_Lon ", weatherStationLocation[1]),
    str_c("WxStation_Lat ", weatherStationLocation[2]),
    str_c("WxStation_Elev ", weatherStationElevation),
    str_c("Wx_file ", weatherFile),
    str_c("Init_hour 13"),
    str_c("FFMC_Method 5"),
    str_c("Threads 1"),
    if (useWindGrid) {
      WindGridParameterStrings
    } else {
      NA
    },
    str_c("Greenup ", GreenUp %>%
      filter(Season %in% c(season, NA)) %>%
      arrange(Season) %>% pull(GreenUp) %>%
      pluck(1) %>%
      as.numeric()),
    if (setGrassCuring) {
      str_c("%Curing_GridFile ", Curing %>%
        filter(Season %in% c(season, NA)) %>%
        arrange(Season) %>%
        pull(FileName) %>%
        pluck(1))
    } else {
      NA
    },
    if (setFuelLoad) {
      str_c("Fuel_Load_GridFile ", FuelLoad %>%
        filter(Season %in% c(season, NA)) %>%
        arrange(Season) %>%
        pull(FileName) %>%
        pluck(1))
    } else {
      NA
    },
    str_c("Duration  ", max(data$BurnDay) * 24L - 1),
    str_c("Export_Every ", max(data$BurnDay) * 24L - 1)
  ) %>%
    discard(is.na)


  # Choose which outputs to save based on chosen output options
  if (OutputOptionsSpatial$BurnPerimeter) {
    parameterFileText <- parameterFileText %>%
      c(str_c("Out_ShapeFiles ", file.path(shapeOutputFolder, fileTag), "_"))
  }
  if (saveBurnMaps) {
    parameterFileText <- parameterFileText %>%
      c(
        str_c("Out_GridFiles ", file.path(gridOutputFolder, fileTag)),
        str_c("Out_Components ", outputComponents)
      )
  }

  # Save parameter file
  writeLines(parameterFileText, parameterFile)

  return(fileTag)
}

### Wrapper functions ----

# Function to grow a single fire
growFire <- function(Iteration, FireID, Season, data, Lat, Lon) {
  # Check if enough fires have been sampled for the given iteration
  if (fireCount < DeterministicIgnitionCount[as.character(Iteration)]) {
    # Indicate progress
    message(str_c("Running Iteration: ", Iteration, "; Fire ID: ", FireID))

    # Generate relevant input files
    generateWeatherFile(data)
    fileTag <- generateParameterFile(Iteration, FireID, Season, Lat, Lon, data)

    # Grow the given fire
    runPandora()

    # Pandora occassionally doesn't produce an output. Possibly when there is truly no burn?
    burnRasterFile <- str_c(gridOutputFolder, "/", fileTag, "_burn.asc")
    if (file.exists(burnRasterFile)) {
      # Read in the burn grid, reclassify zero as NA, 1 as Fire ID
      burnRaster <- rast(burnRasterFile)
      crs(burnRaster) <- crs(fuelsRaster)

      if (OutputOptionsSpatial$AllPerim) {
        writeRaster(burnRaster,
          filename = str_c(allPerimOutputFolder, "/", fileTag, ".tif"),
          overwrite = T,
          NAflag = -9999,
          wopt = list(
            filetype = "GTiff",
            datatype = "INT4S",
            gdal = c("COMPRESS=DEFLATE", "ZLEVEL=9", "PREDICTOR=2")
          )
        )
      }

      area <- freq(burnRaster, value = 1, usenames = T)$count

      # Check if fire meets minimum size
      if (area >= minimumFireSize) {
        # Update count of valid fires
        fireCount <<- fireCount + 1

        burnRaster <- classify(burnRaster, rcl = matrix(c(0L, NA_integer_), nrow = 1))

        # Update burnAccumulator in the parent function, ie runIteration()
        burnAccumulator <<- cover(burnAccumulator, burnRaster)

        for (component in outputComponentsToKeep) {
          inputComponentFileName <- str_c(gridOutputFolder, "/", fileTag, "_", lookup(component, outputComponentNames, outputComponentCodes), ".asc")
          if (file.exists(inputComponentFileName)) {
            # Generate output file name
            outputComponentFileName <- file.path(secondaryOutputFolder, basename(inputComponentFileName) %>% str_replace("asc", "tif"))

            # Rewrite as GeoTiff to output folder
            rast(inputComponentFileName) %>%
              writeRaster(outputComponentFileName,
                overwrite = T,
                NAflag = -9999,
                wopt = list(
                  filetype = "GTiff",
                  datatype = "INT4S",
                  gdal = c("COMPRESS=DEFLATE", "ZLEVEL=9", "PREDICTOR=2")
                )
              )

            # Update corresponding table in SyncroSim
            outputComponentTables[[component]] <<- rbind(
              outputComponentTables[[component]],
              data.frame(
                Iteration = Iteration,
                Timestep = FireID, # TODO: Separate out timestep and fire ID
                FireID = FireID,
                FileName = outputComponentFileName
              )
            )
          }
          unlink(inputComponentFileName)
        }
      }

      # Remove the individual burn raster to limit disk usage
      unlink(burnRasterFile)

      # Return burn area in pixels
      # Note that burned pixels are still encoded as FireID
      return(area)
    }

    # If no output file is found, return a burn area of 0 pixels
    return(0)
  } else {
    return(NA_real_)
  }
}

# Function to grow all the fires in a single iteration
runIteration <- function(Iteration, data) {
  # Report progress
  progressBar("report", iteration = Iteration, timestep = 0)

  # Reset the global burn accumulator
  burnAccumulator <<- setValues(burnAccumulator, rep(NA_integer_, ncell(fuelsRaster)))

  # Grow each fire and get the burned area in pixels
  # Use resolution to convert to hectares
  area <- pmap_dbl(data, growFire, Iteration = Iteration) %>%
    `*`(xres(fuelsRaster) * yres(fuelsRaster) / 1e4)

  # Save burn map to file if user chose to do so or if needed for another map
  if (saveBurnMaps) {
    # All non-burned sites in burn accumulator are currently NA
    # To show map boundary, replace all NA with zero and (later) mask by fuels map
    burnAccumulator <- classify(burnAccumulator, rcl = matrix(c(NA_integer_, 0L), ncol = 2))

    # Save map of accumulated burn maps
    burnMap <- burnAccumulator %>%
      mask(fuelsRaster) %>%
      writeRaster(str_c(accumulatorOutputFolder, "/it", Iteration, ".tif"),
        overwrite = T,
        NAflag = -9999,
        wopt = list(
          filetype = "GTiff",
          datatype = "INT4S",
          gdal = c("COMPRESS=DEFLATE", "ZLEVEL=9", "PREDICTOR=2")
        )
      )
  }

  # Reset fire count
  fireCount <<- 0

  # Update SyncroSim progress bar
  progressBar()

  # Return areas
  return(area)
}

updateRunLog("Finished parsing run inputs in ", updateBreakpoint())

# Prepare shared inputs ----

# Reformat fuel lookup table
FuelType %>%
  transmute(
    grid_value = ID,
    export_value = ID,
    descriptive_name = str_c(Name),
    fuel_type = Code
  ) %>%
  mutate(r = 0, g = 0, b = 0, h = 0, s = 0, l = 0) %>%
  write_csv(fuelLookup, escape = "none")

# Setup dummy location for weather station in the middle of the extent
weatherStationLocation <- latlonFromRowCol(fuelsRaster, floor(nrow(fuelsRaster) / 2), floor(ncol(fuelsRaster) / 2))
weatherStationElevation <- ifelse(!is.null(elevationRaster), elevationRaster[floor(nrow(elevationRaster) / 2), floor(ncol(elevationRaster) / 2)], 0)

# Convert ignition location to lat/long
# Keep only indexes and location
ignitionLocation <- DeterministicIgnitionLocation %>%
  dplyr::select(Iteration, FireID, Season, Lat = Latitude, Lon = Longitude)

# Copy wind grids if needed
if (useWindGrid) {
  # Todo: ensure filepaths are full, copy to temp folder?
  WindGridParameterStrings <- WindGrid %>%
    pivot_longer(cols = -"WindSpeed", names_to = c("Variable", "Direction"), names_prefix = "Wind", values_to = "FileName", names_pattern = "([SD][a-z]*)([NSEW].*)") %>%
    transmute(
      parameter = lookup(Variable, c("Speed", "Direction"), c("WSgrid", "WDgrid")),
      sector = lookup(Direction, c("North", "NorthEast", "East", "SouthEast", "South", "SouthWest", "West", "NorthWest"), 1:8),
      speed = WindSpeed,
      file = FileName %>% str_replace_all("\\\\", "/")
    ) %>%
    unite("parameterFileLine", sep = " ") %>%
    pull()
}

# Decide which burn components to keep
# - Parse table
outputComponentsToKeep <- OutputOptionsSpatialPrometheus %>%
  pivot_longer(everything(), names_to = "component", values_to = "keep") %>%
  filter(keep) %>%
  pull(component)

# - Translate to input keywords as expected by Pandora, prepend burn map keyword
outputComponents <- outputComponentsToKeep %>%
  lookup(old = outputComponentNames, new = outputComponentCodes) %>%
  str_c(collapse = " ") %>%
  str_c("burn ", .)

# - Initialize list of tables to hold outputs
outputComponentTables <- list()
for (component in outputComponentsToKeep) {
  outputComponentTables[[component]] <- data.frame()
}

# Generate fuel loading maps if used
# - It seems that pandora can only set fuel loading using geotiffs, so these must be created based on the season-specific fuel loading value chosen by the user
# - tempfile is used to catch season names that are not acceptable as filenames
if (setFuelLoad) {
  FuelLoad <- FuelLoad %>%
    mutate(FileName = map_chr(Season, ~ tempfile(pattern = "FuelLoad-", tmpdir = tempDir, fileext = ".tif")))

  maskValues <- FuelType %>%
    filter(str_detect(Code, "O-1")) %>%
    pull(ID)

  for (i in seq(nrow(FuelLoad))) {
    rast(fuelsRaster, vals = FuelLoad$FuelLoad[i]) %>%
      mask(fuelsRaster, inverse = T, maskvalues = maskValues) %>%
      writeRaster(FuelLoad$FileName[i],
        overwrite = T,
        NAflag = -9999,
        wopt = list(
          filetype = "GTiff",
          datatype = "FLT4S",
          gdal = c("COMPRESS=DEFLATE", "ZLEVEL=9", "PREDICTOR=2")
        )
      )
  }
}

# Generate grass curing maps if used
# - It seems that pandora can only set fuel loading using geotiffs, so these must be created based on the season-specific fuel loading value chosen by the user
# - tempfile is used to catch season names that are not acceptable as filenames
if (setGrassCuring) {
  Curing <- Curing %>%
    mutate(FileName = map_chr(Season, ~ tempfile(pattern = "Curing-", tmpdir = tempDir, fileext = ".tif")))

  for (i in seq(nrow(Curing))) {
    rast(fuelsRaster, vals = Curing$Curing[i]) %>%
      writeRaster(Curing$FileName[i],
        overwrite = T,
        NAflag = -9999,
        wopt = list(
          filetype = "GTiff",
          datatype = "FLT4S",
          gdal = c("COMPRESS=DEFLATE", "ZLEVEL=9", "PREDICTOR=2")
        )
      )
  }
}

# Combine deterministic input tables ----
fireGrowthInputs <- DeterministicBurnCondition %>%
  # Only consider iterations this job is responsible for
  filter(Iteration %in% iterations) %>%
  # Group by iteration and fire ID for the `growFire()` function
  group_by(Iteration, FireID) %>%
  nest() %>%
  # Add ignition location information
  left_join(ignitionLocation, c("Iteration", "FireID")) %>%
  # Group by just iteration for the `runIteration()` function
  group_by(Iteration) %>%
  nest()

updateRunLog("Finished generating model inputs in ", updateBreakpoint())

progressBar("begin", totalSteps = length(iterations))

# Grow fires ----
progressBar(type = "message", message = "Growing fires...")

burnAreas <- fireGrowthInputs %>%
  pmap(runIteration) %>%
  # The list of burned areas is nested by iteration, we don't need this structure
  unlist()

# Report status ----
updateRunLog(
  "\nBurn Summary:\n",
  sum(!is.na(burnAreas)), " fires burned.\n",
  sum(burnAreas < minimumFireSize, na.rm = T), " fires discarded due to insufficient burn area.\n",
  round(sum(burnAreas >= minimumFireSize, na.rm = T) / sum(!is.na(burnAreas)) * 100, 0), "% of simulated fires were above the minimum fire size.\n"
)

# Issue warning if there were not enough valid fires
if (sum(burnAreas >= minimumFireSize, na.rm = T) < sum(DeterministicIgnitionCount)) {
  # Identify the number of incomplete iterations
  incompleteIterations <- DeterministicIgnitionLocation %>%
    dplyr::select(Iteration) %>%
    mutate(Area = burnAreas) %>%
    group_by(Iteration) %>%
    summarize(ValidFireCount = sum(Area > minimumFireSize, na.rm = T)) %>%
    left_join(
      enframe(DeterministicIgnitionCount, name = "Iteration", value = "IgnitionTarget") %>% mutate(Iteration = as.double(Iteration)),
      by = "Iteration"
    ) %>%
    mutate(Incomplete = ValidFireCount < IgnitionTarget) %>%
    pull(Incomplete) %>%
    sum()

  updateRunLog("Could not sample enough fires above the specified minimum fire size for ", incompleteIterations,
    " iterations. Please increase the Maximum Number of Fires to Resample per Iteration in the Run Controls",
    " or decrease the Minimum Fire Size. Please see the Fire Statistics table for details on specific iterations,",
    " fires, and burn conditions.\n",
    type = "warning"
  )
}

updateRunLog("Finished burning fires in ", updateBreakpoint())

# Save relevant outputs ----

## Fire statistics table ----
if (OutputOptions$FireStatistics) {
  progressBar(type = "message", message = "Generating fire statistics table...")

  # Load necessary rasters and lookup tables
  fireZoneRaster <- tryCatch(
    rast(datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["FireZoneGridFileName"]]),
    error = function(e) NULL
  ) %>%
    checkSpatialInput("Fire Zone", warnOnly = T)
  weatherZoneRaster <- tryCatch(
    rast(datasheet(myScenario, "burnP3Plus_LandscapeRasters")[["WeatherZoneGridFileName"]]),
    error = function(e) NULL
  ) %>%
    checkSpatialInput("Weather Zone", warnOnly = T)
  FireZoneTable <- datasheet(myScenario, "burnP3Plus_FireZone")
  WeatherZoneTable <- datasheet(myScenario, "burnP3Plus_WeatherZone")

  # Built fire statistics table
  OutputFireStatistic <-
    # Start by summarizing burn conditions
    DeterministicBurnCondition %>%
    # Only consider iterations this job is responsible for
    filter(Iteration %in% iterations) %>%
    # Summarize burn conditions by fire
    group_by(Iteration, FireID) %>%
    summarize(
      FireDuration = max(BurnDay),
      HoursBurning = sum(HoursBurning)
    ) %>%
    ungroup() %>%
    mutate(Area = burnAreas) %>%
    # Determine Fire and Weather Zones if the rasters are present, as well as fuel type of ignition location
    left_join(DeterministicIgnitionLocation, by = c("Iteration", "FireID")) %>%
    mutate(
      cell = cellFromLatLong(fuelsRaster, Latitude, Longitude),
      FireZone = ifelse(!is.null(fireZoneRaster), fireZoneRaster[][cell] %>% lookup(FireZoneTable$ID, FireZoneTable$Name), ""),
      WeatherZone = ifelse(!is.null(weatherZoneRaster), weatherZoneRaster[][cell] %>% lookup(WeatherZoneTable$ID, WeatherZoneTable$Name), ""),
      FuelType = fuelsRaster[cell] %>% pull() %>% lookup(FuelType$ID, FuelType$Name)
    ) %>%
    # Finally incorporate Lat and Long and add TimeStep manually
    mutate(
      Timestep = 0,
      ResampleStatus = case_when(
        Area >= minimumFireSize ~ "Kept",
        Area < minimumFireSize ~ "Discarded",
        is.na(Area) ~ "Not Used"
      )
    ) %>%
    # Clean up
    dplyr::select(Iteration, Timestep, FireID, Latitude, Longitude, Season, Cause, FireZone, WeatherZone, FuelType, FireDuration, HoursBurning, Area, ResampleStatus) %>%
    as.data.frame()

  # Output if there are records to save
  if (nrow(OutputFireStatistic) > 0) {
    saveDatasheet(myScenario, OutputFireStatistic, "burnP3Plus_OutputFireStatistic", append = T)
  }

  updateRunLog("Finished collecting fire statistics in ", updateBreakpoint())
}

## Burn maps ----
if (saveBurnMaps) {
  progressBar(type = "message", message = "Saving burn maps...")

  # Build table of burn maps and save to SyncroSim
  OutputBurnMap <-
    tibble(
      FileName = list.files(accumulatorOutputFolder, pattern = "*.tif$", full.names = T),
      Iteration = str_extract(FileName, "\\d+.tif") %>% str_sub(end = -5) %>% as.integer(),
      Timestep = 0
    ) %>%
    filter(Iteration %in% iterations) %>%
    as.data.frame()

  # Output if there are records to save
  if (nrow(OutputBurnMap) > 0) {
    saveDatasheet(myScenario, OutputBurnMap, "burnP3Plus_OutputBurnMap", append = T)
  }

  # Output secondary outputs if present
  if (length(outputComponentsToKeep) > 0) {
    for (i in seq_along(outputComponentTables)) {
      if (nrow(outputComponentTables[[i]]) > 0) {
        saveDatasheet(myScenario, outputComponentTables[[i]], str_c("burnP3PlusPrometheus_Output", outputComponentsToKeep[i], "Map"))
      }
    }
  }

  updateRunLog("Finished collecting burn maps in ", updateBreakpoint())
}

## Burn perimeters ----
if (OutputOptionsSpatial$BurnPerimeter) {
  progressBar(type = "message", message = "Saving burn perimeters...")
  OutputBurnPerimeter <-
    tibble(
      FileName = list.files(shapeOutputFolder, pattern = "*.shp", full.names = T),
      Tag = str_extract(FileName, "it\\d+\\.fid\\d+"),
      Iteration = str_extract(Tag, "it\\d+") %>% str_sub(3) %>% as.integer(),
      FireID = str_extract(Tag, "fid\\d+") %>% str_sub(4) %>% as.integer(),
      Timestep = 0
    ) %>%
    filter(Iteration %in% iterations) %>%
    dplyr::select(-Tag) %>%
    as.data.frame()

  # Output if there are records to save
  if (nrow(OutputBurnPerimeter) > 0) {
    saveDatasheet(myScenario, OutputBurnPerimeter, "burnP3Plus_OutputFirePerimeter", append = T)
  }

  updateRunLog("Finished collecting burn perimeters in ", updateBreakpoint())
}

## All Perims ----
if (OutputOptionsSpatial$AllPerim) {
  progressBar(type = "message", message = "Saving individual burn maps...")

  # Build table of burn maps and save to SyncroSim
  OutputAllPerim <-
    tibble(
      FileName = list.files(allPerimOutputFolder, pattern = "*.tif", full.names = T),
      Iteration = str_extract(FileName, "it\\d+") %>% str_sub(3) %>% as.integer(),
      FireID = str_extract(FileName, "fid\\d+") %>% str_sub(4) %>% as.integer(),
      Timestep = FireID
    ) %>%
    filter(Iteration %in% iterations) %>%
    as.data.frame()

  # Output if there are records to save
  if (nrow(OutputAllPerim) > 0) {
    saveDatasheet(myScenario, OutputAllPerim, "burnP3Plus_OutputAllPerim", append = T)
  }

  updateRunLog("Finished individual burn maps in ", updateBreakpoint())
}

# Remove grid outputs if present
unlink(gridOutputFolder, recursive = T, force = T)
progressBar("end")
