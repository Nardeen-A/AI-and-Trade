# NOTES: ####
# one file may be corrupted (postings_unified_individual_0324_part_04), the data is good quality from 2016 - 2024.

library(sf)
library(tigris)
library(dplyr)
library(ggplot2)
library(stringr)
library(viridis)
library(scales)

options(tigris_use_cache = TRUE)

# ---- data ----
# population data from the US census

ds <- open_dataset("E:\\data\\academic_postings_unified_individual")

pop_2010_2020 <-
  read.csv("C:\\Users\\12898\\Desktop\\Trefler\\data\\pop_2010_2020.csv")
pop_2020_2024 <-
  read.csv("C:\\Users\\12898\\Desktop\\Trefler\\data\\pop_2020_2024.csv")


# ---- data pulling and transformation ----
jobs_by_year_state <- ds %>%
  select(country, state, post_date) %>%
  filter(country == "United States",!is.na(state)) %>%
  mutate(post_date = as.Date(post_date),   # Arrow-supported cast
         year = year(post_date)) %>%
  group_by(year, state) %>%
  summarise(n_jobs = n(), .groups = "drop") %>%
  collect()

# the code above takes about 15 minutes to run

# saving job_by_year_state before filtering our sparse years
jobs_by_year_state <- jobs_by_year_state %>%
  arrange(year)
  
  write.csv(
    jobs_by_year_state,
    "C:\\Users\\12898\\Desktop\\Trefler\\output\\data\\data_sparsity.csv",
    row.names = FALSE
  )


jobs_by_year_state <- jobs_by_year_state %>%
  filter(!(year %in% c(2008:2016, 2025))) %>%
  filter(state != "empty") %>%
  mutate(state = ifelse(state == "Washington, D.C.",
                        "District of Columbia",
                        state))

# population data
pop_2010_2020 <- pop_2010_2020[-c(1:5, 57), -c(1:4, 6, 7, 18, 19)]
pop_2020_2024 <- pop_2020_2024[-c(1:14, 66), c(5, 7:11)]

colnames(pop_2010_2020)[1] <- "state"
colnames(pop_2020_2024)[1] <- "state"

to_long <- function(df) {
  df %>%
    pivot_longer(
      cols = matches("^POPESTIMATE\\d{4}$"),
      names_to = "year",
      values_to = "population"
    ) %>%
    mutate(year = as.integer(sub("POPESTIMATE", "", year)),
           population = as.numeric(population))
}

pop_long_all <- bind_rows(to_long(pop_2010_2020),
                          to_long(pop_2020_2024)) %>%
  distinct(state, year, .keep_all = TRUE) %>%  # drops duplicate overlap years if any
  arrange(state, year)


# merging
jobs_pop <- jobs_by_year_state %>%
  left_join(pop_long_all, by = c("state", "year")) %>%
  mutate(jobs_per_cap = n_jobs / population)


# maps ####
# ---- 2023 data ----
jobs_2023 <- jobs_pop %>%
  mutate(
    year = as.integer(year),
    state = str_trim(state),
    state = if_else(
      state %in% c("Washington, D.C.", "Washington, D.C. "),
      "District of Columbia",
      state
    )
  ) %>%
  filter(year == 2023) %>%
  mutate(jobs_per_10k = jobs_per_cap * 10000,
         log_jobs_per_10k = log(jobs_per_10k))

# ---- States geometry: drop AK/HI to avoid squishing ----
states_sf <-
  tigris::states(cb = TRUE, year = 2022, class = "sf") %>%
  filter(STUSPS %in% c(state.abb, "DC")) %>%
  filter(!NAME %in% c("Alaska", "Hawaii")) %>%   # key line
  transmute(state = NAME, geometry) %>%
  st_transform(5070)  # nice US Albers projection

# ---- plot 2023 ----
map_df_2023 <- states_sf %>%
  left_join(jobs_2023, by = "state")

p_2023 <- ggplot(map_df_2023) +
  geom_sf(aes(fill = log_jobs_per_10k),
          color = "white",
          linewidth = 0.2) +
  scale_fill_viridis_c(option = "mako", na.value = "grey92") +
  labs(title = "Job postings per 18+ by state (2023)",
       fill  = element_blank()) +
  coord_sf(datum = NA) +
  theme_void(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "right",
    plot.margin = margin(10, 10, 10, 10)
  )

# ---- facet plot ----

# ---- Prepare jobs data (ALL years) ----
jobs_all <- jobs_pop %>%
  mutate(
    year = as.integer(year),
    state = str_trim(state),
    state = if_else(
      state %in% c("Washington, D.C.", "Washington, D.C."),
      "District of Columbia",
      state
    ),
    jobs_per_10k = jobs_per_cap * 10000,
    log_jobs_per_10k = log(jobs_per_10k)
  ) %>%
  group_by(year) %>%
  mutate(
    jobs_pct = percent_rank(log_jobs_per_10k)     # 0..1 within each year) %>%
    ungroup()
    
    # ---- Join geometries to data ----
    map_df_all <- states_sf %>%
      left_join(jobs_all, by = "state") %>%
      filter(!is.na(year))
    
    # ---- Faceted plot (percentile scale fixed 0..1 across years) ----
    p_facetted <- ggplot(map_df_all) +
      geom_sf(
        aes(fill = jobs_pct),
        color = "white",
        linewidth = 0.2
      ) +
      facet_wrap(~ year, ncol = 3) +
      scale_fill_viridis_c(
        option = "mako",
        limits = c(0, 1),
        labels = scales::percent,
        na.value = "grey92"
      ) +
      labs(title = "Job postings intensity per capita by state (within-year percentile)",
           fill  = "Percentile") +
      coord_sf(datum = NA) +
      theme_void(base_size = 12) +
      theme(
        plot.title = element_text(face = "bold", hjust = 0.5),
        strip.text = element_text(face = "bold"),
        legend.position = "bottom",
        plot.margin = margin(10, 10, 10, 10)
      )
    
    p_facetted
    
    # savings data frames
    
    write.csv(
      jobs_all,
      "C:\\Users\\12898\\Desktop\\Trefler\\output\\data\\jobs_by_year_state_population.csv",
      row.names = FALSE
    )
    
    write.csv(
      pop_long_all,
      "C:\\Users\\12898\\Desktop\\Trefler\\output\\data\\population.csv",
      row.names = FALSE
    )
    