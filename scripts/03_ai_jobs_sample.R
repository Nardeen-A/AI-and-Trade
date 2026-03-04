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

ai_rows <- ds %>%
  filter(role_k1500 %in% ai_categories) %>%
  select(rcid, title_translated, country, state, post_date) %>%
  collect()




# 1️⃣ Clean titles (fast + safe)
ai_rows <- ai_rows %>%
  mutate(
    title_clean = title_translated %>%
      str_to_lower() %>%
      str_replace_all("[^a-z0-9]+", " ") %>%
      str_squish()
  )


# 2️⃣ Core AI terms only
core_terms <- c(
  "artificial intelligence",
  "machine learning",
  "deep learning",
  "computer vision",
  "natural language processing",
  "reinforcement learning",
  "generative ai",
  "large language model",
  "neural network",
  "neural networks",
  "\\bai\\b",
  "\\bml\\b",
  "\\bnlp\\b",
  "\\bllm\\b"
)

core_ai_pattern <- paste0("(?i)(", paste(core_terms, collapse = "|"), ")")


# 4️⃣ Flag AI jobs (vectorized, fast)
ai_rows$is_core_ai <- stri_detect_regex(ai_rows$title_clean, core_ai_pattern)


# 5️⃣ Extract AI-only subset (optional)
df_core_ai <- ai_rows %>%
  filter(is_core_ai == TRUE)

write.csv(
  df_core_ai,
  file.path(proj.path, "output", "data", "ai_jobs_sample.csv"),
  row.names = FALSE
)

write.csv(
  ai_rows,
  file.path(proj.path, "output", "data", "ai_adj_with_job_titles.csv"),
  row.names = FALSE
)
