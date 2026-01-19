# reading in files
proj.path <- getwd()

ds <- open_dataset("G:\\data\\academic_postings_unified_individual")

pop_2010_2020 <-
  read.csv(file.path(proj.path, "data", "pop_2010_2020.csv"))
pop_2020_2024 <-
  read.csv(file.path(proj.path, "data", "pop_2020_2024.csv"))


# population data ####
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


# unique job categories ####
job_categories <- ds %>%
  select(role_k1500) %>%   # ONLY the column you need
  distinct() %>%             # Arrow pushes this down
  collect()

nrow(job_categories)
View(job_categories)

# categories 
ai_categories <- c(
  "data scientist",
  "data engineer",
  "data science",
  "statistician",
  "automation engineer",
  "research",
  "researcher",
  "research scientist",
  "research engineer",
  "research analyst",
  "research development",
  "research development engineer",
  "scientific",
  "scientist",
  "scientist i",
  "computer scientist",
  "innovation",
  "software engineer",
  "software engineer i",
  "software engineering",
  "software development",
  "software development engineer",
  "software development engineer test",
  "software engineer analyst",
  "software engineering analyst",
  "software architect",
  "advisory software engineer",
  "software designer",
  "software consultant",
  "software programmer",
  "developer",
  "programmer",
  "programmer analyst",
  "computer programmer",
  "sde",
  "sdet",
  "sw engineer",
  "python developer",
  "java developer",
  "java software developer",
  "java software engineer",
  "android developer",
  "web developer",
  "web application developer",
  "frontend developer",
  "ui developer",
  "net developer",
  "embedded software engineer",
  "system software engineer",
  "devops",
  "devops engineer",
  "site reliability engineer",
  "cloud engineer",
  "cloud architect",
  "infrastructure engineer",
  "infrastructure architect",
  "infrastructure",
  "it infrastructure",
  "systems engineer",
  "systems engineering",
  "system engineering",
  "systems architect",
  "system architect",
  "storage engineer",
  "data center",
  "deployment",
  "etl developer",
  "database engineer",
  "database analyst",
  "database developer",
  "database administrator",
  "data architect",
  "qa automation engineer",
  "test automation engineer"
  
)

ai_categories <- intersect(ai_categories, job_categories$role_k1500)

ai_rows <- ds %>%
  filter(role_k1500 %in% ai_categories) %>%
  select(rcid, rics_k400, role_k1500, location_raw, country, state, post_date) %>%
  collect()

# data is saved!

ai_rows <- ai_rows %>%
  select(c(3, 5, 6, 7))

role_count <- ai_rows %>%
  filter(country == "United States") %>%
  group_by(role_k1500, country, state, post_date) %>%
  summarise(
    n_individuals = n(),
    .groups = "drop"
  ) %>%
  collect()

ai_count <- ai_rows %>%
  filter(country == "United States") %>%
  group_by(state, post_date) %>%
  summarise(
    n_postings = n(),
    n_unique_roles = n_distinct(role_k1500),
    .groups = "drop"
  ) %>%
  mutate(post_date = as.Date(post_date),   # Arrow-supported cast
         year = year(post_date)) %>%
  select(c(1, 3, 5)) %>%
  arrange(year) %>%
  group_by(year, state) %>%
  summarise(n_jobs = sum(n_postings, na.rm = TRUE), .groups = "drop")

ai_count <- ai_count %>%
  filter(!(year %in% c(2008:2016, 2025))) %>%
  filter(state != "empty") %>%
  mutate(state = ifelse(state == "Washington, D.C.",
                        "District of Columbia",
                        state))

ai_per_cap <- ai_count %>%
  left_join(pop_long_all, by = c("state", "year")) %>%
  mutate(jobs_per_cap = n_jobs / population)



# map 
states_sf <-
  tigris::states(cb = TRUE, year = 2022, class = "sf") %>%
  filter(STUSPS %in% c(state.abb, "DC")) %>%
  filter(!NAME %in% c("Alaska", "Hawaii")) %>%   # key line
  transmute(state = NAME, geometry) %>%
  st_transform(5070)  # nice US Albers projection

# ---- Prepare jobs data (ALL years) ----
maps_data <- ai_per_cap %>%
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
    jobs_pct = percent_rank(log_jobs_per_10k))%>%
    ungroup()
    
    # ---- Join geometries to data ----
    map_df_all <- states_sf %>%
      left_join(maps_data, by = "state") %>%
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
      labs(title = "AI adjecent job postings intensity per capita by state (within-year percentile)",
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
    


write.csv(
  ai_per_cap,
  file.path(proj.path, "output", "data", "ai_per_cap.csv"),
  row.names = FALSE
)


