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

server <- function(input, output, session) {
  
  # Función para leer archivos desde OneDrive
  read_csv_onedrive <- function(url) {
    temp_file <- tempfile(fileext = ".csv")
    download.file(url, temp_file, mode = "wb")
    read.csv(temp_file)
  }
  
  # URLs de los archivos en OneDrive
  url_hist <- "https://bitsmaxwarehouse-my.sharepoint.com/personal/ecalderon_maxwarehouse_com/_layouts/15/download.aspx?share=EdiGicgPVWZPqh8NIBhAmNcBixr9ZZ7-qXA6V0ruvhGySg"
  
  url_pred <- "https://bitsmaxwarehouse-my.sharepoint.com/personal/ecalderon_maxwarehouse_com/_layouts/15/download.aspx?share=Ee7PfajUv1BCic3fxWUGaecBLYzl8dE5T46gkrm6lHyo_w"
  
  url_met <- "https://bitsmaxwarehouse-my.sharepoint.com/personal/ecalderon_maxwarehouse_com/_layouts/15/download.aspx?share=EZqERYd6S7JCuIXLJ5Ydp6ABhhVqpqGXlhMVIQGV8GI16Q"
  
  url_cuant <- "https://bitsmaxwarehouse-my.sharepoint.com/personal/ecalderon_maxwarehouse_com/_layouts/15/download.aspx?share=EWfz45gXO0tGkAq05LMcGqwBM-4r-wg-bVDOWkDutzRFXg"
  
  
  url_costo <- "https://bitsmaxwarehouse-my.sharepoint.com/personal/ecalderon_maxwarehouse_com/_layouts/15/download.aspx?share=EQ6KsNeP1DRGreWKLDjbGYsB7imePw9K3wfnuBsVdSBhhA"
  
  
  # Descargar los archivos al inicio
  historico_data <- read_csv_onedrive(url_hist)
  predicciones_data <- read_csv_onedrive(url_pred)
  metricas_data <- read_csv_onedrive(url_met)
  cuant_data <- read_csv_onedrive(url_cuant)
  costo_data <- read_csv_onedrive(url_costo)
  
  # Crear reactives para que los datos sean accesibles en la aplicación
  historico <- reactive({ historico_data })
  predicciones <- reactive({ predicciones_data })
  metricas <- reactive({ metricas_data })
  cuant <- reactive({ cuant_data })
  costo <- reactive({ costo_data })
  
  # ----------------------------------Filtros--------------------------------#
  
  observe({
    updateSelectizeInput(session, "sku", 
                         choices = unique(historico()$Sku))
  })
  
  # Filtro dinámico de UomCode según el Sku seleccionado
  observeEvent(input$sku, {
    req(input$sku)  # Asegurar que hay un SKU seleccionado
    
    # Filtrar UomCode disponibles según el SKU seleccionado
    uom_choices <- historico() %>%
      filter(Sku == input$sku) %>%
      pull(UomCode_N) %>%
      unique()
    
    # Agregar opción vacía para permitir ver todos los UomCode si no se elige uno
    uom_choices <- c("", uom_choices)
    
    # Actualizar el filtro de UomCode en la UI
    updateSelectInput(session, "uom", choices = uom_choices, selected = "")
  })
  
  observe({
    updateSelectInput(session, "weight_flag_filter",
                      choices = unique(metricas()$Weight_Flag))
  })
  
  observe({
    updateSelectInput(session, "dim_flag_filter",
                      choices = unique(metricas()$DIM_Flag))
  })
  
  # Crear los filtros para fechas y semanas
  observe({
    updateSelectInput(session, "date_filter", choices = unique(cuant()$Fecha))  
    updateSelectInput(session, "week_filter", choices = unique(cuant()$Semana)) 
  })
  
  observe({
    updateSelectInput(session, "date_filter2", 
                      choices = unique(costo()$InvDate))  
    updateSelectInput(session, "week_filter2", 
                      choices = unique(costo()$Semana)) 
  })
  
  #-------------PRIMERA PESTAÑA---------------------------
  
  # Filtrar datos históricos según el SKU y opcionalmente el UomCode
  filtered_hist <- reactive({
    req(input$sku)
    
    data <- historico() %>% filter(Sku == input$sku)
    
    if (input$uom != "") {
      data <- data %>% filter(UomCode == input$uom)
    }
    data
  })
  
  # Filtrar predicción según el SKU y opcionalmente el UomCode
  filtered_pred <- reactive({
    req(input$sku)
    
    data <- predicciones() %>% filter(Sku == input$sku)
    
    if (input$uom != "") {
      data <- data %>% filter(UomCode_N == input$uom)
    }
    data
  })
  
  # Mostrar el SKU seleccionado
  output$sku_seleccionado <- renderUI({
    HTML(paste("<p style='font-size: 18px; font-weight: bold;'>", input$sku, "</p>"))
  })
  
  # Mostrar la tabla de datos históricos
  output$hist_table <- renderDT({
    datatable(
      filtered_hist() %>%
        select(TranDate, InvDate, trackingnum, Sku, UomCode_N, UomQuantity,
               Weight, Height, Length, Width),
      extensions = 'Buttons',
      options = list(
        dom = 'lBfrtip',
        buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
        lengthMenu = list(c(10, 25, 50, 100, -1), 
                          c("10", "25", "50", "100", "Todos")),
        pageLength = 10,
        autoWidth = TRUE,
        searchHighlight = TRUE
      ),
      filter = 'top'
    )
  })
  
  # Botón descarga histórico
  output$download_hist <- downloadHandler(
    filename = function() {
      paste("historial_skus_", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      write.csv(filtered_hist() %>%
                  select(TranDate, InvDate, trackingnum, Sku, UomCode_N, 
                         Weight, Height, Length, Width),
                file, row.names = FALSE)
    }
  )
  
  # Histogramas de datos históricos
  output$hist_weight <- renderPlot({
    ggplot(filtered_hist(), aes(x = Weight)) +
      geom_histogram(binwidth = 0.1, fill = "steelblue", color = "black") +
      labs(title = "Distribución de Peso", x = "Peso", y = "Frecuencia") +
      theme_minimal()
  })
  
  output$hist_height <- renderPlot({
    ggplot(filtered_hist(), aes(x = Height)) +
      geom_histogram(binwidth = 0.1, fill = "darkgreen", color = "black") +
      labs(title = "Distribución de Altura", x = "Altura", y = "Frecuencia") +
      theme_minimal()
  })
  
  output$hist_length <- renderPlot({
    ggplot(filtered_hist(), aes(x = Length)) +
      geom_histogram(binwidth = 0.1, fill = "darkorange", color = "black") +
      labs(title = "Distribución de Longitud", x = "Longitud", y = "Frecuencia") +
      theme_minimal()
  })
  
  output$hist_width <- renderPlot({
    ggplot(filtered_hist(), aes(x = Width)) +
      geom_histogram(binwidth = 0.1, fill = "purple", color = "black") +
      labs(title = "Distribución de Ancho", x = "Ancho", y = "Frecuencia") +
      theme_minimal()
  })
  
  
  #-------------SEGUNDA PESTAÑA---------------------------
  
  
  # Tabla Predicciones Ajustadas
  output$pred_table_ajustada <- renderTable({
    req(nrow(filtered_pred()) > 0)
    df3 <- filtered_pred() %>% 
      select(Sku, UomCode, UomQuantity, Final_Weight, Final_Height, 
             Final_Length, Final_Width)
    setNames(df3, c("Sku", "UomCode", "UomQuantity", "Final Weight", 
                    "Final Height", "Final Length", "Final Width"))
  })
  
  # Botón descarga predicciones
  output$download_pred <- downloadHandler(
    filename = function() {
      paste("predicciones_skus_", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      req(nrow(filtered_pred()) > 0)  # Asegura que haya datos antes de descargar
      write.csv(filtered_pred() %>% 
                  select(Sku, UomCode_N, Final_Weight, Final_Height, 
                         Final_Length, Final_Width), 
                file, row.names = FALSE)
    })
  
  
  
  #-------------TERCERA PESTAÑA---------------------------
  
  # Cálculo de cantidad y porcentaje para Weight_Flag
  weight_summary <- reactive({
    metricas() %>%
      count(Weight_Flag) %>%
      mutate(Percentage = n / sum(n) * 100)
  })
  
  # Cálculo de cantidad y porcentaje para DIM_Flag
  dim_summary <- reactive({
    metricas() %>%
      count(DIM_Flag) %>%
      mutate(Percentage = n / sum(n) * 100)
  })
  
  # Gráfico de cumplimiento en peso (Weight_Flag) con porcentaje
  output$grafico_peso <- renderPlot({
    ggplot(weight_summary(), aes(x = Weight_Flag, y = n, fill = Weight_Flag)) +
      geom_bar(stat = "identity") +
      geom_text(aes(label = paste0(n, " (", round(Percentage, 1), "%)")), 
                vjust = -0.5, size = 5) +
      labs(title = "Distribución de SKUs por Weight_Flag", 
           x = "Categoría", y = "Cantidad") +
      theme_minimal()
  })
  
  # Gráfico de cumplimiento en dimensiones (DIM_Flag) con porcentaje
  output$grafico_dimensiones <- renderPlot({
    ggplot(dim_summary(), aes(x = DIM_Flag, y = n, fill = DIM_Flag)) +
      geom_bar(stat = "identity") +
      geom_text(aes(label = paste0(n, " (", round(Percentage, 1), "%)")), 
                vjust = -0.5, size = 5) +
      labs(title = "Distribución de SKUs por DIM_Flag", 
           x = "Categoría", y = "Cantidad") +
      theme_minimal()
  })
  
  # Filtrar datos de cumplimiento según Weight_Flag y DIM_Flag seleccionados
  filtered_compliance <- reactive({
    data <- metricas()
    
    if (!is.null(input$weight_flag_filter) && length(input$weight_flag_filter) > 0) {
      data <- data %>% filter(Weight_Flag %in% input$weight_flag_filter)
    }
    
    if (!is.null(input$dim_flag_filter) && length(input$dim_flag_filter) > 0) {
      data <- data %>% filter(DIM_Flag %in% input$dim_flag_filter)
    }
    
    return(data)
  })
  
  # Tabla con los datos filtrados de cumplimiento
  output$compliance_table <- renderDT({
    req(nrow(filtered_compliance()) > 0)
    
    # Obtener los nombres de las últimas 7 columnas
    last_columns <- tail(names(filtered_compliance()), 5)
    last_col <- last_columns[length(last_columns)]
    
    datatable(
      filtered_compliance() %>% 
        select(Sku, Weight, EJD_Weight, th_weight, Sum_Dim, Sum_Dim_EJD_Low,
               Sum_Dim_EJD_Sup, Weight_Flag, DIM_Flag, all_of(last_columns)),
      colnames = c("Sku", "Median Weight", "EJD Weight LL", "EJD Weight UL",
                   "Median Sum Dims", "EJD Sum Dims LL", "EJD Sum Dims UL",
                   "Weight Flag", "Dims Flag", last_columns),
      extensions = c('Buttons'),
      options = list(
        dom = 'lBfrtip',
        buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
        lengthMenu = list(c(10, 25, 50, 100, -1), 
                          c("10", "25", "50", "100", "Todos")),
        pageLength = 10
      ),
      rownames = FALSE
    ) %>% 
      formatStyle(
        c("Weight", "EJD_Weight", "th_weight"), 
        backgroundColor = "rgba(52, 152, 219, 0.5)") %>%
      formatStyle( 
        c("Sum_Dim", "Sum_Dim_EJD_Low", "Sum_Dim_EJD_Sup"), 
        backgroundColor = "rgba(46, 204, 113, 0.5)") %>%
      formatStyle(
        last_col,  # Última columna dinámica
        color = styleInterval(-0.00001, c("red", "black"))
      )
  })
  
  
  #-------------CUARTA PESTAÑA---------------------------
  
  # Llenado de filtros
  observe({
    dataC <- cuant()
    updateSelectInput(session, "date_filter", 
                      choices = sort(unique(as.character(dataC$Fecha))),
                      selected = NULL)
    
    updateSelectInput(session, "week_filter", 
                      choices = sort(unique(as.character(dataC$Semana))),
                      selected = NULL)
  })
  
  # Filtrar la base de datos reactiva según la fecha y semana seleccionada
  cuant_filtrado <- reactive({
    dataC <- cuant()  
    dataC$Fecha <- as.Date(dataC$Fecha)
    dataC$Semana <- as.character(dataC$Semana)
    
    # Filtrar por fechas seleccionadas (una o más)
    if (!is.null(input$date_filter) && length(input$date_filter) > 0) {
      fechas_seleccionadas <- as.Date(input$date_filter)
      dataC <- dataC %>%
        dplyr::filter(Fecha %in% fechas_seleccionadas)
    }
    
    # Filtrar por semanas seleccionadas
    if (!is.null(input$week_filter) && length(input$week_filter) > 0) {
      dataC <- dataC %>%
        dplyr::filter(Semana %in% input$week_filter)
    }
    
    return(dataC)
  })
  
  # Mostrar los datos filtrados en las tarjetas
  
  # Cantidad de Skus
  output$skus_totales <- renderValueBox({
    data_filtrada <- cuant_filtrado()
    total_skus <- length(unique(data_filtrada$Sku))
    
    valueBox(
      value = tags$h3(total_skus, style = "font-weight: bold;"),
      subtitle = tags$h4("SKUs Totales", style = "font-weight: lighter;"),
      icon = icon("cubes"),  
      color = "light-blue")
  })
  
  # SKUs que subieron peso
  output$skus_subieron_peso <- renderValueBox({
    data_filtrada <- cuant_filtrado() 
    skus_subieron_peso <- sum(data_filtrada$variacion_weight > 0, na.rm = TRUE)
    
    valueBox(
      value = tags$h3(skus_subieron_peso, style = "font-weight: bold;"),
      subtitle = tags$h4("SKUs Subieron Peso", style = "font-weight: lighter;"),
      icon = icon("arrow-up"),
      color = "green")
  })
  
  # SKUs que bajaron peso
  output$skus_bajaron_peso <- renderValueBox({
    data_filtrada <- cuant_filtrado() 
    skus_bajaron_peso <- sum(data_filtrada$variacion_weight < 0, na.rm = TRUE)
    
    valueBox(
      value = tags$h3(skus_bajaron_peso, style = "font-weight: bold;"),
      subtitle = tags$h4("SKUs Bajaron Peso", style = "font-weight: lighter;"),
      icon = icon("arrow-down"),
      color = "red")
  })
  
  # SKUs que subieron dimensiones
  output$skus_subieron_dims <- renderValueBox({
    data_filtrada <- cuant_filtrado() 
    skus_subieron_dims <- sum(data_filtrada$variacion_dims > 0, na.rm = TRUE)
    
    valueBox(
      value = tags$h3(skus_subieron_dims, style = "font-weight: bold;"),
      subtitle = tags$h4("SKUs Subieron Dimensiones", 
                         style = "font-weight: lighter;"),
      icon = icon("expand"),
      color = "aqua")
  })
  
  # SKUs que bajaron dimensiones
  output$skus_bajaron_dims <- renderValueBox({
    data_filtrada <- cuant_filtrado() 
    skus_bajaron_dims <- sum(data_filtrada$variacion_dims < 0, na.rm = TRUE)
    
    valueBox(
      value = tags$h3(skus_bajaron_dims, style = "font-weight: bold;"),
      subtitle = tags$h4("SKUs Bajaron Dimensiones", 
                         style = "font-weight: lighter;"),
      icon = icon("compress"),
      color = "orange")
  })
  
  # Tabla de detalle
  output$tabla_variaciones <- DT::renderDataTable({
    data_filtrada <- cuant_filtrado()
    
    data_tabla <- data_filtrada %>%
      dplyr::select(Fecha, Semana, Sku, UomCode, variacion_weight,
                    variacion_dims) %>%
      dplyr::mutate(
        peso_status = dplyr::case_when(
          variacion_weight > 0 ~ "Subió Peso",
          variacion_weight < 0 ~ "Bajó Peso",
          TRUE ~ "Sin Cambio"
        ),
        dims_status = dplyr::case_when(
          variacion_dims > 0 ~ "Subió Dimensiones",
          variacion_dims < 0 ~ "Bajó Dimensiones",
          TRUE ~ "Sin Cambio"
        )
      )
    
    DT::datatable(
      data_tabla,
      filter = "top",
      selection = "single",
      extensions = 'Buttons',
      options = list(pageLength = 10, autoWidth = TRUE, searchHighlight = TRUE,
                     dom = 'lBfrtip', 
                     buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                     lengthMenu = list(
                       c(10, 25, 50, 100, -1),
                       c('10', '25', '50', '100', 'Todos'))
      ),
      rownames = FALSE
    ) %>%
      DT::formatStyle(
        'peso_status',
        target = 'cell',
        backgroundColor = DT::styleEqual(
          c("Subió Peso", "Bajó Peso", "Sin Cambio"),
          c('lightgreen', '#F08080', 'white')
        )
      ) %>%
      DT::formatStyle(
        'dims_status',
        target = 'cell',
        backgroundColor = DT::styleEqual(
          c("Subió Dimensiones", "Bajó Dimensiones", "Sin Cambio"),
          c('lightgreen', '#F08080', 'white')
        )
      )
  })
  
  # Panel expandible para mostrar detalles
  output$detalles_expandible <- renderUI({
    selected <- input$tabla_variaciones_rows_selected
    
    if (length(selected)) {
      # Si hay un SKU seleccionado
      data_filtrada <- cuant_filtrado()
      
      sku_seleccionado <- data_filtrada %>%
        dplyr::select(
          Sku, Final_Weight, Final_Height, Final_Length, Final_Width,
          EVP_Weight,	EVP_Height, EVP_Length,	EVP_Width
        ) %>%
        dplyr::slice(selected)
      
      sku_nombre <- sku_seleccionado$Sku
      
      box(
        title = paste("Detalles del SKU:", sku_nombre),
        status = "info",
        solidHeader = TRUE,
        collapsible = TRUE,
        width = 12,
        DT::dataTableOutput("detalle_tabla")
      )
    } else {
      # <--- AGREGADO
      NULL
    }
  })
  
  # Renderiza la tabla de detalles aparte
  output$detalle_tabla <- DT::renderDataTable({
    selected <- input$tabla_variaciones_rows_selected
    
    if (length(selected)) {
      data_filtrada <- cuant_filtrado()
      sku_seleccionado <- data_filtrada %>%
        dplyr::select(
          Sku, Final_Weight, Final_Height, Final_Length, Final_Width,
          EVP_Weight,	EVP_Height, EVP_Length,	EVP_Width
        ) %>%
        dplyr::slice(selected)
      
      DT::datatable(
        sku_seleccionado,
        options = list(dom = 't'),  # Solo tabla, sin buscador ni paginador
        rownames = FALSE
      )
    }
  })
  
  #-------------QUINTA PESTAÑA---------------------------
  # Llenado de filtros
  observe({
    dataC <- costo() 
    updateSelectInput(session, "date_filter2", 
                      choices = sort(unique(as.character(dataC$InvDate))),
                      selected = NULL)
    
    updateSelectInput(session, "week_filter2", 
                      choices = sort(unique(as.character(dataC$Semana))),
                      selected = NULL)
  })
  
  # Filtrar la base de datos reactiva según la fecha y semana seleccionada
  costo_filtrado <- reactive({
    dataC <- costo()  
    dataC$InvDate	 <- as.Date(dataC$InvDate)
    dataC$Semana <- as.character(dataC$Semana)
    
    # Filtrar por fechas seleccionadas
    if (!is.null(input$date_filter2) && length(input$date_filter2) > 0) {
      fechas_sel <- as.Date(input$date_filter2)
      dataC <- dataC %>% dplyr::filter(InvDate %in% fechas_sel)
    }
    
    # Filtrar por semanas seleccionadas
    if (!is.null(input$week_filter2) && length(input$week_filter2) > 0) {
      dataC <- dataC %>% dplyr::filter(Semana %in% input$week_filter2)
    }
    
    # Cálculo de variación
    dataC <- dataC %>%
      mutate(
        variacion_costo = Cost - EstimatedCost,
        porcentaje_desviacion = ifelse(EstimatedCost != 0, 
                                       (variacion_costo / EstimatedCost), NA)
      )
    return(dataC)
  })
  
  # Mostrar los datos filtrados en las tarjetas
  
  # SKUs arriba del estimated
  output$skus_arriba <- renderValueBox({
    data_filtrada <- costo_filtrado() 
    skus_arriba <- sum(data_filtrada$variacion_costo > 0, na.rm = TRUE)
    
    valueBox(
      value = tags$h3(skus_arriba, style = "font-weight: bold;"),
      subtitle = tags$h4("Trackings con Shipping arriba del Estimated", 
                         style = "font-weight: lighter;"),
      icon = icon("arrow-up"),
      color = "red")
  })
  
  # SKUs debajo del estimated
  output$skus_abajo <- renderValueBox({
    data_filtrada <- costo_filtrado() 
    skus_abajo <- sum(data_filtrada$variacion_costo < 0, na.rm = TRUE)
    
    valueBox(
      value = tags$h3(skus_abajo, style = "font-weight: bold;"),
      subtitle = tags$h4("Trackings con Shipping debajo del Estimated", 
                         style = "font-weight: lighter;"),
      icon = icon("arrow-down"),
      color = "green")
  })
  
  # Costo estimado total
  output$costo_estimado <- renderValueBox({
    data_filtrada <- costo_filtrado()
    costo_estimado <- sum(data_filtrada$EstimatedCost, na.rm = TRUE)
    
    valueBox(
      value = tags$h4(scales::dollar(costo_estimado), 
                      style = "font-weight: bold; font-size: 24px;"),
      subtitle = "Costo Estimado Total",
      icon = icon("calculator"),
      color = "light-blue")
  })
  
  # Costo real total
  output$costo_real <- renderValueBox({
    data_filtrada <- costo_filtrado()
    costo_real <- sum(data_filtrada$Cost, na.rm = TRUE)
    
    valueBox(
      value = tags$h4(scales::dollar(costo_real), 
                      style = "font-weight: bold; font-size: 24px;"),
      subtitle = "Costo Real Total",
      icon = icon("money-bill-wave"),
      color = "orange")
  })
  
  # Diferencia total
  output$desviacion <- renderValueBox({
    data_filtrada <- costo_filtrado()
    costo_estimado <- sum(data_filtrada$EstimatedCost, na.rm = TRUE)
    costo_real <- sum(data_filtrada$Cost, na.rm = TRUE)
    diferencia <- costo_real - costo_estimado
    porcentaje <- ifelse(costo_estimado != 0, 
                         (diferencia / costo_estimado) * 100, NA)
    
    valueBox(
      value = paste0(round(porcentaje, 2), "%"),
      subtitle = "Diferencia Total",
      icon = icon("balance-scale"),
      color = ifelse(diferencia >= 0, "olive", "red")
    )
  })
  
  # Tabla de detalle de costos
  
  # Tabla de variación positiva
  output$tabla_positiva <- DT::renderDataTable({
    req(costo_filtrado()) 
    
    data_filtrada <- costo_filtrado()
    
    data_positiva <- data_filtrada %>%
      dplyr::filter(variacion_costo > 0) %>%
      dplyr::select(InvDate, Semana, trackingnum, FulfillmentOrderNumber, Sku, UomCode,
                    EstimatedCost, Cost) %>%
      dplyr::mutate(
        variacion_costo_pct = ifelse(EstimatedCost != 0,
                                     scales::percent((Cost - EstimatedCost) / EstimatedCost, accuracy = 0.01), NA)) %>%
      dplyr::arrange(desc((Cost - EstimatedCost) / EstimatedCost))
    
    DT::datatable(
      data_positiva,
      filter = "top",
      selection = "single",
      options = list(pageLength = 10, autoWidth = TRUE, searchHighlight = TRUE,
                     dom = 'lBfrtip', 
                     buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                     lengthMenu = list(
                       c(10, 25, 50, 100, -1),
                       c('10', '25', '50', '100', 'Todos'))
      ),
      rownames = FALSE) 
  })
  
  # Tabla de variación negativa
  output$tabla_negativa <- DT::renderDataTable({
    req(costo_filtrado()) 
    
    data_filtrada <- costo_filtrado()
    
    data_negativa <- data_filtrada %>%
      dplyr::filter(variacion_costo < 0) %>%
      dplyr::select(InvDate, Semana, trackingnum, FulfillmentOrderNumber, Sku, UomCode,
                    EstimatedCost, Cost) %>%
      dplyr::mutate(
        variacion_costo_pct = ifelse(EstimatedCost != 0,
                                     scales::percent((Cost - EstimatedCost) / EstimatedCost, accuracy = 0.01),NA)) %>%
      dplyr::arrange((Cost - EstimatedCost) / EstimatedCost)
    
    DT::datatable(
      data_negativa,
      filter = "top",
      selection = "single",
      options = list(pageLength = 10, autoWidth = TRUE, searchHighlight = TRUE,
                     dom = 'lBfrtip', 
                     buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
                     lengthMenu = list(
                       c(10, 25, 50, 100, -1),
                       c('10', '25', '50', '100', 'Todos'))
      ),
      rownames = FALSE)
  })
  
  
  # Cerrar conexión al salir
  onStop(function() {
    dbDisconnect(con)
  })
}
