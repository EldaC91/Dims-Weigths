library(shiny)
library(DT)
library(ggplot2)
library(dplyr)
library(plotly)
library(shinydashboard)
library(shinydashboardPlus)
library(shinyjs)
library(Microsoft365R)
library(httr)
library(DBI)
library(odbc)

ui <- dashboardPage(
  
  dashboardHeader(
    title = "Predicción de Pesos y Medidas",
    titleWidth = 400),
  
  # Sidebar: Filtros y selección de SKU
  dashboardSidebar(
    collapsed = TRUE,
    sidebarMenu(
      menuItem("Información Histórica", tabName = "historico", 
               icon = icon("table")),
      menuItem("Predicciones", tabName = "predicciones", 
               icon = icon("chart-line")),
      menuItem("Cumplimiento de Medidas", tabName = "cumplimiento", 
               icon = icon("check-circle")),
      menuItem("Cuantificación Skus", tabName = "cuantificacion", 
               icon = icon("balance-scale")),
      menuItem("Costos", tabName = "costos", 
               icon = icon("dollar"))
    ),
    # Filtro SKU en el sidebar
    selectizeInput("sku", "Seleccionar SKU:", choices = NULL, multiple = FALSE),
    selectizeInput("uom", "Seleccionar UomCode:", choices = NULL, 
                   multiple = FALSE)),
  
  # Body: Contenido principal de las pestañas
  dashboardBody(
    
    tabItems(
      
      # -----------------Primera pestaña: Información Histórica---------------#
      tabItem("historico",
              fluidRow(
                column(12, 
                       h3("Datos Históricos del SKU"),
                       uiOutput("sku_seleccionado"),
                       downloadButton("download_hist", "Descargar CSV"),
                       DT::dataTableOutput("hist_table")
                )
              ),
              
              fluidRow(
                column(12, 
                       h3("Distribuciones Peso y Medidas")
                )
              ),
              
              # Fila 2: Gráficos
              fluidRow(
                column(6, plotOutput("hist_weight")),
                column(6, plotOutput("hist_height"))
              ),
              
              # Fila 3: Gráficos
              fluidRow(
                column(6, plotOutput("hist_length")),
                column(6, plotOutput("hist_width"))
              )
      ),
      
      #-----------------------Segunda pestaña: Predicciones------------------#
      tabItem("predicciones",
              fluidRow(
                column(12,
                       h3("Predicciones Peso y Dimensiones"),
                       downloadButton("download_pred", "Descargar CSV"),
                       tableOutput("pred_table_ajustada")
                )
              )
      ),
      
      #------------------Tercera pestaña: Gráficos de Cumplimiento------------#
      tabItem("cumplimiento",
              fluidRow(
                column(6,
                       selectInput("weight_flag_filter", "Filtrar por Weight_Flag:",
                                   choices = NULL,
                                   multiple = TRUE)),
                column(6,
                       selectInput("dim_flag_filter", "Filtrar por DIM_Flag:", 
                                   choices = NULL,
                                   multiple = TRUE))
              ),
              
              fluidRow(
                column(6, 
                       h3("Distribucion Skus Peso"), 
                       plotOutput("grafico_peso")),
                column(6, 
                       h3("Distribucion Skus Dims"),
                       plotOutput("grafico_dimensiones"))
              ),
              
              fluidRow(
                column(12, 
                       h3("Lista de Skus por Categoria"),
                       DTOutput("compliance_table"))
              )
      ),
      
      #------------------Cuarta pestaña: Cuantificación----------------------#
      
      tabItem("cuantificacion",
              fluidRow(
                column(6,
                       selectInput("date_filter", "Filtrar por Fecha:",
                                   choices = NULL,
                                   multiple = TRUE)),
                column(6,
                       selectInput("week_filter", "Filtrar por Semana:", 
                                   choices = NULL,
                                   multiple = TRUE))
              ),
              
              # Tarjetas (value boxes) para mostrar los datos filtrados
              fluidRow(
                box(
                  title = "Resumen de Variaciones", status = "primary", solidHeader
                  = TRUE, width = 12,
                  fluidRow(
                    valueBoxOutput("skus_totales", width = 2),
                    valueBoxOutput("skus_subieron_peso", width = 2),
                    valueBoxOutput("skus_bajaron_peso", width = 2),
                    valueBoxOutput("skus_subieron_dims", width = 2),
                    valueBoxOutput("skus_bajaron_dims", width = 2)
                  )
                )
              ),
              fluidRow(
                column(12, 
                       h3("Tabla de Detalle de Variaciones"), 
                       DT::dataTableOutput("tabla_variaciones"))
              ),
              br(),
              fluidRow(
                column(12,
                       uiOutput("detalles_expandible"))
              )
      ),
      
      #------------------Quinta pestaña: Costos----------------------#
      tabItem("costos",
              fluidRow(
                column(6,
                       selectInput("date_filter2", "Filtrar por Fecha:",
                                   choices = NULL,
                                   multiple = TRUE)),
                column(6,
                       selectInput("week_filter2", "Filtrar por Semana:", 
                                   choices = NULL,
                                   multiple = TRUE))
              ),
              
              # Tarjetas para mostrar los datos filtrados
              fluidRow(
                box(
                  title = "Resumen de Variaciones", status = "primary", solidHeader
                  = TRUE, width = 12,
                  fluidRow(
                    valueBoxOutput("skus_arriba", width = 2),
                    valueBoxOutput("skus_abajo", width = 2),
                    valueBoxOutput("costo_real", width = 2),
                    valueBoxOutput("costo_estimado", width = 2),
                    valueBoxOutput("desviacion", width = 2)
                  )
                )
              ),
              
              fluidRow(
                column(10, 
                       h3("Variación Positiva (Costo real mayor al estimado)"),
                       div(
                         style = "background-color: #ffe6e6; padding: 15px;
                   border-radius: 10px;",
                         DT::dataTableOutput("tabla_positiva")
                       ))
              ),
              fluidRow(
                column(10, 
                       h3("Variación Negativa (Costo real menor al estimado)"), 
                       div(
                         style = "background-color: #e6ffe6; padding: 15px;
                   border-radius: 10px;",
                         DT::dataTableOutput("tabla_negativa")
                       ))
              )
      )
    )
  )
)