suppressPackageStartupMessages({
  library(shiny)
  library(bslib)
  library(DBI)
  library(RSQLite)
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(ggplot2)
  library(plotly)
  library(scales)
  library(glue)
})

DB_PATH <- normalizePath(file.path("..", "db", "health_advisory.db"), mustWork = FALSE)
TARGET_CROSSING_DATE <- as.Date("2027-07-12")
CAL_START <- as.Date("2026-03-09")

mi_to_yd <- function(mi) round(mi * 1760)

build_training_calendar <- function(start_date = CAL_START) {
  long_miles <- c(2.0, 2.5, 2.3, 3.0, 3.5, NA, 3.0, 4.0)
  med_miles  <- c(1.2, 1.5, 1.8, 2.1, 1.8, 2.1, 2.4, 2.1)

  rows <- list()
  for (wk in seq_len(8)) {
    mon <- start_date + days((wk - 1) * 7)
    travel_weekend <- wk %in% c(4, 6)

    rows[[length(rows) + 1]] <- tibble(
      date = mon,
      week = wk,
      day_name = "Mon",
      plan = glue("Lift Day 1 (Chest/Triceps/Shoulder prehab/Core) + Medium-long swim {med_miles[wk]} mi ({comma(mi_to_yd(med_miles[wk]))} yd)")
    )
    rows[[length(rows) + 1]] <- tibble(
      date = mon + days(1),
      week = wk,
      day_name = "Tue",
      plan = "Lift Day 2 (Back/Lats/Biceps/Scap/Core) + Easy/Technique swim (30–40 min)"
    )
    rows[[length(rows) + 1]] <- tibble(
      date = mon + days(2),
      week = wk,
      day_name = "Wed",
      plan = "Quality swim (hard, shorter): 8x100 or 5x200"
    )
    rows[[length(rows) + 1]] <- tibble(
      date = mon + days(3),
      week = wk,
      day_name = "Thu/Fri",
      plan = "One day: Lift Day 3 (Legs/Arms/Mobility) + Easy swim (30–45 min). Other day: Recovery/mobility walk"
    )

    sat_plan <- if (travel_weekend) {
      "Travel weekend: No swim"
    } else {
      glue("Long swim {long_miles[wk]} mi ({comma(mi_to_yd(long_miles[wk]))} yd)")
    }

    sun_plan <- if (travel_weekend) {
      "Travel weekend: No swim"
    } else {
      "Easy recovery swim (20–35 min)"
    }

    rows[[length(rows) + 1]] <- tibble(
      date = mon + days(5),
      week = wk,
      day_name = "Sat",
      plan = sat_plan
    )
    rows[[length(rows) + 1]] <- tibble(
      date = mon + days(6),
      week = wk,
      day_name = "Sun",
      plan = sun_plan
    )
  }

  bind_rows(rows) %>% mutate(month = format(date, "%B %Y"))
}

load_whoop <- function(path) {
  con <- dbConnect(SQLite(), path)
  on.exit(dbDisconnect(con), add = TRUE)
  dbGetQuery(con, "
    SELECT day, recovery_score, sleep_performance, strain
    FROM whoop_daily
    ORDER BY day
  ") %>%
    mutate(day = as.Date(day))
}

load_swim <- function(path) {
  con <- dbConnect(SQLite(), path)
  on.exit(dbDisconnect(con), add = TRUE)
  dbGetQuery(con, "
    SELECT day, distance_value, unit
    FROM swim_daily
    ORDER BY day
  ") %>%
    mutate(
      day = as.Date(day),
      unit = tolower(unit),
      yards = case_when(
        unit %in% c('yd','yard','yards') ~ as.numeric(distance_value),
        unit %in% c('m','meter','meters') ~ as.numeric(distance_value) * 1.09361,
        unit %in% c('mi','mile','miles') ~ as.numeric(distance_value) * 1760,
        TRUE ~ as.numeric(distance_value)
      )
    )
}

range_filter <- function(df, preset) {
  if (!nrow(df)) return(df)
  if (is.null(preset) || !nzchar(preset)) preset <- "30D"
  end <- max(df$day, na.rm = TRUE)
  start <- switch(
    preset,
    "1D" = end,
    "3D" = end - days(2),
    "7D" = end - days(6),
    "14D" = end - days(13),
    "30D" = end - days(29),
    "90D" = end - days(89),
    "ALL" = min(df$day, na.rm = TRUE),
    end - days(29)
  )
  df %>% filter(day >= start, day <= end)
}

ui <- page_navbar(
  title = "Health Dashboard v1",
  theme = bs_theme(version = 5, bg = "#0b1020", fg = "#dbeafe", primary = "#60a5fa"),
  header = tags$style(HTML("
    .glass-card {
      background: linear-gradient(145deg, rgba(30,41,59,.75), rgba(15,23,42,.55));
      border: 1px solid rgba(148,163,184,.22);
      border-radius: 16px;
      padding: 14px 16px;
      box-shadow: 0 10px 26px rgba(2,6,23,.35);
    }
    .glass-label { color:#93c5fd; font-size:.82rem; text-transform:uppercase; letter-spacing:.08em; }
    .glass-value { font-size:2rem; font-weight:700; line-height:1.1; margin:6px 0; }
    .glass-row { display:flex; gap:8px; flex-wrap:wrap; margin-top:8px; }
    .chip { background: rgba(148,163,184,.18); border:1px solid rgba(148,163,184,.3); border-radius:999px; padding:4px 10px; font-size:.8rem; }
    .card, .card-body, .bslib-card { overflow: visible !important; }
    select, .form-select { width: 100% !important; max-width: 100% !important; }
    .shiny-input-container { margin-bottom: 10px; }
  ")),

  nav_panel(
    "Overview",
    card(
      card_header("Range + Overlay Controls"),
      div(style = "margin-bottom:10px;",
        selectInput("overview_range", "Range", choices = c("1D","3D","7D","14D","30D","90D","ALL"), selected = "30D", selectize = FALSE)
      ),
      div(style = "margin-bottom:6px;",
        checkboxGroupInput("overlay_metrics", "Overlay lines (off by default)", choices = c("Recovery" = "recovery", "Sleep" = "sleep"), selected = character(0))
      ),
      checkboxInput("show_mean", "Show strain mean", value = TRUE)
    ),
    p(class = "text-secondary", textOutput("overview_window")),
    uiOutput("overview_hero"),
    uiOutput("overview_reco"),
    card(
      full_screen = TRUE,
      card_header("Daily Strain Bars + Overlays"),
      plotlyOutput("trend_plot", height = "420px")
    )
  ),

  nav_panel(
    "Swim Progress",
    card(
      card_header("Range"),
      selectInput("swim_range", NULL, choices = c("1D","7D","14D","30D"), selected = "30D", selectize = FALSE)
    ),
    p(class = "text-secondary", textOutput("swim_window")),
    uiOutput("swim_hero"),
    uiOutput("swim_reco"),
    card(
      full_screen = TRUE,
      card_header("Catalina → Long Beach"),
      plotlyOutput("swim_map", height = "430px")
    )
  ),

  nav_panel(
    "Training Calendar",
    card(
      card_header("8-Week Swim + Lift Calendar (starts Mar 9, 2026)"),
      p(class = "text-secondary", "Thursday/Friday are merged: one day is Lift Day 3 + easy swim, the other is recovery. Swap as needed."),
      actionButton("add_workout", "Add Workout", class = "btn btn-primary btn-sm"),
      tags$span(" "),
      actionButton("toggle_archive", "Show Archive", class = "btn btn-outline-light btn-sm"),
      tags$div(style = "height:8px;"),
      uiOutput("calendar_view"),
      uiOutput("archive_view")
    ),
  ),

  nav_panel(
    "7D Snapshot",
    card(
      card_header("Previous Rolling 7 Days"),
      p(class = "text-secondary", textOutput("snapshot_window")),
      uiOutput("snapshot_reco"),
      uiOutput("snapshot_table")
    )
  ),

  nav_panel(
    "Insights",
    card(
      card_header("Simple callouts"),
      uiOutput("insights")
    )
  )
)

server <- function(input, output, session) {
  whoop_all <- reactivePoll(
    10000, session,
    checkFunc = function() file.info(DB_PATH)$mtime,
    valueFunc = function() load_whoop(DB_PATH)
  )

  swim_all <- reactivePoll(
    10000, session,
    checkFunc = function() file.info(DB_PATH)$mtime,
    valueFunc = function() load_swim(DB_PATH)
  )

  whoop <- reactive(range_filter(whoop_all(), input$overview_range))
  swim <- reactive(range_filter(swim_all(), input$swim_range))

  show_archive <- reactiveVal(FALSE)

  observeEvent(input$toggle_archive, {
    show_archive(!isTRUE(show_archive()))
    updateActionButton(session, "toggle_archive", label = if (isTRUE(show_archive())) "Hide Archive" else "Show Archive")
  })

  save_training_log <- function(day, session_type, yards, mins, note) {
    note_parts <- c()
    if (nzchar(trimws(note))) note_parts <- c(note_parts, trimws(note))
    if (session_type %in% c("Lift", "Swim + Lift")) note_parts <- c(note_parts, "Lift session")
    if (session_type %in% c("Recovery / Rest")) note_parts <- c(note_parts, "Recovery / rest day")
    if (session_type %in% c("Hike", "Pickleball", "Spikeball") && mins > 0) note_parts <- c(note_parts, glue("{session_type}: {round(mins)} min"))
    if (session_type %in% c("Swim", "Swim + Lift") && mins > 0) note_parts <- c(note_parts, glue("Swim: {round(yards)} yd in {round(mins)} min"))

    con <- dbConnect(SQLite(), DB_PATH)
    on.exit(dbDisconnect(con), add = TRUE)

    if (session_type %in% c("Swim", "Swim + Lift") && yards > 0) {
      dbExecute(con, "INSERT INTO swim_daily (day, distance_value, unit, source, raw_text, message_ts) VALUES (?, ?, 'yd', 'manual_calendar', ?, datetime('now'))",
                params = list(day, yards, paste(note_parts, collapse = " | ")))
    }

    if (length(note_parts) > 0) {
      dbExecute(con, "INSERT INTO notes_daily (day, category, note) VALUES (?, 'workout', ?)",
                params = list(day, paste(note_parts, collapse = ". ")))
    }
  }

  open_log_modal <- function(day_value) {
    showModal(modalDialog(
      title = glue("Add Workout — {day_value}"),
      selectInput("modal_log_type", "Session", choices = c("Swim", "Lift", "Swim + Lift", "Hike", "Pickleball", "Spikeball", "Recovery / Rest"), selected = "Swim"),

      conditionalPanel(
        condition = "input.modal_log_type == 'Lift' || input.modal_log_type == 'Swim + Lift'",
        textInput("modal_lift_type", "Lift type", value = "", placeholder = "e.g. Back day")
      ),

      conditionalPanel(
        condition = "input.modal_log_type == 'Swim' || input.modal_log_type == 'Swim + Lift'",
        numericInput("modal_log_yards", "Swim yards", value = 0, min = 0, step = 25)
      ),

      numericInput("modal_log_minutes", "Duration (minutes)", value = 0, min = 0, step = 1),
      textAreaInput("modal_log_note", "Notes", value = "", placeholder = "Optional notes"),
      footer = tagList(
        modalButton("Cancel"),
        actionButton("modal_log_save", "Save to DB", class = "btn btn-primary")
      ),
      easyClose = TRUE,
      size = "m"
    ))
  }

  observeEvent(input$add_workout, {
    picked <- Sys.Date()
    showNotification(glue("Add workout for {picked}"), type = "default", duration = 2)
    open_log_modal(picked)
  })

  observeEvent(input$picked_day, {
    picked <- suppressWarnings(as.Date(input$picked_day))
    if (is.na(picked)) return()
    open_log_modal(picked)
  })

  observeEvent(input$modal_log_save, {
    req(input$modal_log_type)

    day_raw <- if (!is.null(input$picked_day)) input$picked_day else as.character(Sys.Date())
    day <- as.character(as.Date(day_raw))
    session_type <- input$modal_log_type
    yards <- suppressWarnings(as.numeric(input$modal_log_yards)); if (is.na(yards)) yards <- 0
    mins <- suppressWarnings(as.numeric(input$modal_log_minutes)); if (is.na(mins)) mins <- 0
    note <- if (is.null(input$modal_log_note)) "" else input$modal_log_note
    lift_type <- if (is.null(input$modal_lift_type)) "" else trimws(input$modal_lift_type)

    if (session_type %in% c("Lift", "Swim + Lift") && nzchar(lift_type)) {
      note <- paste(c(lift_type, note), collapse = if (nzchar(note)) ". " else "")
    }

    save_training_log(day, session_type, yards, mins, note)
    removeModal()
    showNotification(glue("Saved {session_type} for {day}."), type = "message", duration = 3)
  })

  output$overview_window <- renderText({
    d <- whoop()
    if (!nrow(d)) return(glue("Overview range: {input$overview_range} (no data)"))
    glue("Overview range: {input$overview_range} • {min(d$day)} → {max(d$day)} • {nrow(d)} days")
  })

  output$swim_window <- renderText({
    d <- swim()
    if (!nrow(d)) return(glue("Swim range: {input$swim_range} (no data)"))
    glue("Swim range: {input$swim_range} • {min(d$day)} → {max(d$day)} • {nrow(d)} day(s)")
  })

  snapshot_vals <- reactive({
    d <- whoop()
    list(
      recovery = if (!nrow(d)) "N/A" else as.character(round(dplyr::last(na.omit(d$recovery_score)),1)),
      sleep = if (!nrow(d)) "N/A" else paste0(round(dplyr::last(na.omit(d$sleep_performance)),1), "%"),
      strain = if (!nrow(d)) "N/A" else as.character(round(dplyr::last(na.omit(d$strain)),1))
    )
  })

  output$overview_hero <- renderUI({
    v <- snapshot_vals()
    tags$div(class = "glass-card",
      tags$div(class = "glass-label", "Today Snapshot"),
      tags$div(class = "glass-value", glue("Strain {v$strain}")),
      tags$div(class = "glass-row",
        tags$span(class = "chip", glue("Recovery {v$recovery}")),
        tags$span(class = "chip", glue("Sleep {v$sleep}")),
        tags$span(class = "chip", glue("{input$overview_range}"))
      )
    )
  })

  output$swim_hero <- renderUI({
    tags$div(class = "glass-card",
      tags$div(class = "glass-label", "Swim Session"),
      tags$div(class = "glass-value", if (!nrow(swim())) "0 yd" else { end <- max(swim()$day, na.rm = TRUE); paste0(comma(round(sum(swim()$yards[swim()$day >= end - days(6)], na.rm = TRUE),0)), " yd") }),
      tags$div(class = "glass-row",
        tags$span(class = "chip", if (!nrow(swim())) "0%" else paste0(round(min(1, sum(swim()$yards, na.rm = TRUE)/(22*1760))*100,1), "%")),
        tags$span(class = "chip", format(TARGET_CROSSING_DATE, "%b %d, %Y")),
        tags$span(class = "chip", glue("{input$swim_range}"))
      )
    )
  })

  output$recovery_latest <- renderText({
    d <- whoop(); if (!nrow(d)) return("N/A")
    round(dplyr::last(na.omit(d$recovery_score)), 1)
  })
  output$sleep_latest <- renderText({
    d <- whoop(); if (!nrow(d)) return("N/A")
    paste0(round(dplyr::last(na.omit(d$sleep_performance)), 1), "%")
  })
  output$strain_latest <- renderText({
    d <- whoop(); if (!nrow(d)) return("N/A")
    round(dplyr::last(na.omit(d$strain)), 1)
  })

  output$trend_plot <- renderPlotly({
    d <- whoop() %>%
      filter(!is.na(strain)) %>%
      mutate(
        strain_norm = pmin(1, pmax(0, strain / 20)),
        recovery_norm = pmin(1, pmax(0, recovery_score / 100)),
        sleep_norm = pmin(1, pmax(0, sleep_performance / 100))
      )

    validate(need(nrow(d) > 0, "No data in selected range"))

    p <- ggplot(d, aes(x = day)) +
      geom_col(aes(y = strain_norm), fill = "#f97316", alpha = 0.85, width = 0.8) +
      scale_y_continuous(labels = percent_format(), limits = c(0, 1.05)) +
      theme_minimal(base_size = 13) +
      theme(legend.position = "bottom", axis.title = element_blank())

    if ("recovery" %in% input$overlay_metrics) {
      p <- p + geom_line(aes(y = recovery_norm, color = "Recovery"), linewidth = 1.2)
    }
    if ("sleep" %in% input$overlay_metrics) {
      p <- p + geom_line(aes(y = sleep_norm, color = "Sleep"), linewidth = 1.2)
    }
    if (isTRUE(input$show_mean)) {
      mean_strain <- mean(d$strain_norm, na.rm = TRUE)
      p <- p + geom_hline(yintercept = mean_strain, linetype = "dashed", color = "#a78bfa", linewidth = 1) +
        annotate("text", x = min(d$day, na.rm = TRUE), y = mean_strain + 0.03, label = glue("Mean strain: {round(mean_strain*20,1)}"), color = "#a78bfa", hjust = 0, size = 3.5)
    }

    if (length(input$overlay_metrics) > 0) {
      p <- p + scale_color_manual(values = c(Recovery = "#22c55e", Sleep = "#38bdf8"))
    }

    ggplotly(p) %>%
      layout(dragmode = FALSE) %>%
      config(displayModeBar = FALSE, responsive = TRUE, scrollZoom = FALSE)
  })

  output$swim_week <- renderText({
    d <- swim(); if (!nrow(d)) return("0 yd")
    end <- max(d$day, na.rm = TRUE)
    paste0(comma(round(sum(d$yards[d$day >= end - days(6)], na.rm = TRUE), 0)), " yd")
  })

  output$swim_progress <- renderText({
    d <- swim(); if (!nrow(d)) return("0%")
    pct <- min(1, sum(d$yards, na.rm = TRUE) / (22 * 1760))
    paste0(round(pct * 100, 1), "%")
  })

  output$target_date <- renderText(format(TARGET_CROSSING_DATE, "%b %d, %Y"))

  output$swim_map <- renderPlotly({
    d <- swim()
    pct <- if (nrow(d)) min(1, sum(d$yards, na.rm = TRUE) / (22 * 1760)) else 0
    if (!is.finite(pct)) pct <- 0

    # Catalina Avalon Harbor -> Long Beach shoreline reference
    catalina <- c(lat = 33.3436, lng = -118.3267)
    long_beach <- c(lat = 33.7676, lng = -118.1956)
    prog_lat <- catalina[["lat"]] + (long_beach[["lat"]] - catalina[["lat"]]) * pct
    prog_lng <- catalina[["lng"]] + (long_beach[["lng"]] - catalina[["lng"]]) * pct

    plot_ly() %>%
      add_trace(
        type = "scattermapbox",
        mode = "lines",
        lon = c(catalina[["lng"]], long_beach[["lng"]]),
        lat = c(catalina[["lat"]], long_beach[["lat"]]),
        line = list(color = "#60a5fa", width = 4),
        name = "Catalina → Long Beach",
        hoverinfo = "skip"
      ) %>%
      add_trace(
        type = "scattermapbox",
        mode = "markers+text",
        lon = c(catalina[["lng"]], long_beach[["lng"]], prog_lng),
        lat = c(catalina[["lat"]], long_beach[["lat"]], prog_lat),
        text = c("Avalon, Catalina", "", glue("You ({round(pct*100,1)}%)")),
        textposition = c("top right", "top right", "bottom right"),
        marker = list(size = c(10, 10, 14), color = c("#22c55e", "#f97316", "#a78bfa")),
        hovertemplate = c("Avalon, Catalina<extra></extra>", "Long Beach<extra></extra>", glue("You ({round(pct*100,1)}%)<extra></extra>")),
        showlegend = FALSE
      ) %>%
      layout(
        mapbox = list(
          style = "open-street-map",
          zoom = 8.6,
          center = list(lat = 33.56, lon = -118.26)
        ),
        margin = list(l = 0, r = 0, t = 0, b = 0),
        dragmode = FALSE
      ) %>%
      config(displayModeBar = FALSE, responsive = TRUE, scrollZoom = FALSE)
  })

  output$overview_reco <- renderUI({
    d <- whoop_all() %>% arrange(day)
    if (!nrow(d)) return(NULL)

    last <- tail(d, 1)
    rec <- suppressWarnings(as.numeric(last$recovery_score))
    slp <- suppressWarnings(as.numeric(last$sleep_performance))
    strain <- suppressWarnings(as.numeric(last$strain))

    msg <- if (!is.na(rec) && rec < 60) {
      "Recovery is low today: keep swim easy and skip high-intensity lifting."
    } else if (!is.na(slp) && slp < 85) {
      "Sleep is below target: keep volume but reduce intensity 20–30% today."
    } else if (!is.na(strain) && strain > 14) {
      "Strain was high: prioritize technique/easy aerobic and recovery work today."
    } else {
      "Good readiness: complete the planned key session, then protect sleep timing tonight."
    }

    tags$div(class = "glass-card", style = "margin-bottom:12px;",
      tags$div(class = "glass-label", "Today’s Recommendation"),
      tags$div(style = "font-weight:600;", msg)
    )
  })

  output$swim_reco <- renderUI({
    s <- swim_all() %>% arrange(day)
    if (!nrow(s)) return(NULL)
    end <- max(s$day, na.rm = TRUE)
    wk <- sum(s$yards[s$day >= end - days(6)], na.rm = TRUE)
    msg <- if (wk < 4000) {
      "Low recent swim volume: add one medium-long aerobic swim this week."
    } else if (wk > 12000) {
      "High weekly swim load: keep quality capped to one hard day and protect recovery."
    } else {
      "Swim load is in range: keep one quality day + one long day, with easy days truly easy."
    }

    tags$div(class = "glass-card", style = "margin-bottom:12px;",
      tags$div(class = "glass-label", "Swim Next Action"),
      tags$div(style = "font-weight:600;", msg)
    )
  })

  output$snapshot_reco <- renderUI({
    w <- whoop_all() %>% arrange(day)
    if (!nrow(w)) return(NULL)

    last7 <- tail(w, 7)
    prev7 <- tail(head(w, nrow(w) - 7), 7)
    if (!nrow(prev7)) return(NULL)

    rec_delta <- round(mean(last7$recovery_score, na.rm = TRUE) - mean(prev7$recovery_score, na.rm = TRUE), 1)
    slp_delta <- round(mean(last7$sleep_performance, na.rm = TRUE) - mean(prev7$sleep_performance, na.rm = TRUE), 1)

    msg <- if (rec_delta < -5 || slp_delta < -3) {
      glue("This week dipped (Recovery {rec_delta}, Sleep {slp_delta}). Reduce intensity for 48h and tighten sleep consistency.")
    } else if (rec_delta > 3 && slp_delta > 0) {
      glue("This week improved (Recovery +{rec_delta}, Sleep +{slp_delta}). Keep structure and progress one key session.")
    } else {
      glue("Week is stable (Recovery {rec_delta}, Sleep {slp_delta}). Hold volume steady and avoid adding extra hard days.")
    }

    tags$div(class = "glass-card", style = "margin-bottom:12px;",
      tags$div(class = "glass-label", "This Week Adjustment"),
      tags$div(style = "font-weight:600;", msg)
    )
  })

  output$calendar_view <- renderUI({
    invalidateLater(60000, session)  # refresh every minute so midnight rollover auto-updates

    cal <- build_training_calendar(CAL_START)
    today <- Sys.Date()
    upcoming <- cal %>% filter(date >= today)
    months <- unique(upcoming$month)

    tags$div(
      lapply(months, function(m) {
        chunk <- upcoming %>% filter(month == m)
        tags$div(
          class = "glass-card",
          style = "margin-bottom: 14px;",
          tags$h5(style = "margin-bottom:10px; color:#93c5fd;", m),
          lapply(seq_len(nrow(chunk)), function(i) {
            tags$div(
              style = "padding:8px 0; border-top:1px solid rgba(148,163,184,.2);",
              tags$a(
                href = "#",
                style = "font-weight:600; color:#dbeafe; text-decoration:none;",
                onclick = sprintf("Shiny.setInputValue('picked_day','%s',{priority:'event'}); return false;", as.character(chunk$date[i])),
                glue("{format(chunk$date[i], '%b %d')} (W{chunk$week[i]}) • {chunk$day_name[i]}")
              ),
              tags$div(style = "opacity:.95;", chunk$plan[i])
            )
          })
        )
      })
    )
  })

  output$archive_view <- renderUI({
    if (!isTRUE(show_archive())) return(NULL)

    cal <- build_training_calendar(CAL_START)
    today <- Sys.Date()
    archived <- cal %>% filter(date < today) %>% arrange(desc(date))
    if (!nrow(archived)) return(tags$div(class = "text-secondary", "No archived days yet."))

    tags$div(
      class = "glass-card",
      style = "margin-top: 10px;",
      tags$h5(style = "margin-bottom:10px; color:#93c5fd;", "Archive (click day to edit)"),
      lapply(seq_len(nrow(archived)), function(i) {
        tags$div(
          style = "padding:8px 0; border-top:1px solid rgba(148,163,184,.2); opacity:.85;",
          tags$a(
            href = "#",
            style = "font-weight:600; color:#dbeafe; text-decoration:line-through;",
            onclick = sprintf("Shiny.setInputValue('picked_day','%s',{priority:'event'}); return false;", as.character(archived$date[i])),
            glue("✅ {format(archived$date[i], '%b %d')} (W{archived$week[i]}) • {archived$day_name[i]}")
          ),
          tags$div(style = "opacity:.9; text-decoration:line-through;", archived$plan[i])
        )
      })
    )
  })

  output$snapshot_window <- renderText({
    w <- whoop_all()
    s <- swim_all()
    max_day <- max(c(w$day, s$day), na.rm = TRUE)
    if (!is.finite(max_day)) return("No data")
    start_day <- max_day - days(6)
    glue("{start_day} → {max_day}")
  })

  output$snapshot_table <- renderUI({
    w <- whoop_all()
    s <- swim_all()
    max_day <- max(c(w$day, s$day), na.rm = TRUE)
    if (!is.finite(max_day)) return(tags$p("No data for 7-day snapshot."))

    days_tbl <- tibble(day = seq(max_day - days(6), max_day, by = "day"))

    whoop_7 <- w %>%
      select(day, recovery_score, sleep_performance, strain)

    swim_7 <- s %>%
      mutate(swim_yd = round(yards, 0)) %>%
      select(day, swim_yd)

    d <- days_tbl %>%
      left_join(whoop_7, by = "day") %>%
      left_join(swim_7, by = "day") %>%
      mutate(
        day_label = format(day, "%a %b %d"),
        recovery = ifelse(is.na(recovery_score), "-", as.character(round(recovery_score, 0))),
        sleep = ifelse(is.na(sleep_performance), "-", paste0(round(sleep_performance, 0), "%")),
        strain = ifelse(is.na(strain), "-", as.character(round(strain, 1))),
        swim = ifelse(is.na(swim_yd), "-", paste0(comma(swim_yd), " yd"))
      )

    tags$div(
      class = "glass-card",
      style = "padding: 10px 12px;",
      tags$div(style = "font-weight:700; color:#bfdbfe; margin-bottom:8px;", "7-Day Snapshot (single-shot view)"),
      tags$table(
        style = "width:100%; border-collapse:collapse; font-size:0.82rem; line-height:1.2;",
        tags$thead(
          tags$tr(
            style = "color:#93c5fd; border-bottom:1px solid rgba(148,163,184,.25);",
            tags$th(style = "text-align:left; padding:6px 4px;", "Day"),
            tags$th(style = "text-align:right; padding:6px 4px;", "Rec"),
            tags$th(style = "text-align:right; padding:6px 4px;", "Sleep"),
            tags$th(style = "text-align:right; padding:6px 4px;", "Strain"),
            tags$th(style = "text-align:right; padding:6px 4px;", "Swim")
          )
        ),
        tags$tbody(
          lapply(seq_len(nrow(d)), function(i) {
            tags$tr(
              style = "border-bottom:1px solid rgba(148,163,184,.12);",
              tags$td(style = "padding:6px 4px; white-space:nowrap;", d$day_label[i]),
              tags$td(style = "padding:6px 4px; text-align:right;", d$recovery[i]),
              tags$td(style = "padding:6px 4px; text-align:right;", d$sleep[i]),
              tags$td(style = "padding:6px 4px; text-align:right;", d$strain[i]),
              tags$td(style = "padding:6px 4px; text-align:right; white-space:nowrap;", d$swim[i])
            )
          })
        )
      )
    )
  })

  output$insights <- renderUI({
    d <- whoop()
    if (!nrow(d)) return(tags$p("No data in selected range."))

    r <- mean(tail(d$recovery_score, 7), na.rm = TRUE)
    s <- mean(tail(d$sleep_performance, 7), na.rm = TRUE)
    st <- mean(tail(d$strain, 7), na.rm = TRUE)

    tags$ul(
      tags$li(glue("7-day avg Recovery: {round(r,1)}")),
      tags$li(glue("7-day avg Sleep: {round(s,1)}%")),
      tags$li(glue("7-day avg Strain: {round(st,2)}"))
    )
  })
}

shinyApp(ui, server)
