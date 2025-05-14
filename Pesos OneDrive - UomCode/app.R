# Cargar el archivo ui.R
source("ui.R")

# Definir el servidor en server.R
source("server.R")

# Ejecutar la app
shinyApp(ui = ui, server = server)