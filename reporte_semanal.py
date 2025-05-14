import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
# Calcular lunes y domingo de la semana anterior
hoy = datetime.today()
lunes_pasado = hoy - timedelta(days=hoy.weekday() + 7)
domingo_pasado = lunes_pasado + timedelta(days=6)

def sufijo(dia):
    return 'st' if dia in [1, 21, 31] else \
           'nd' if dia in [2, 22] else \
           'rd' if dia in [3, 23] else 'th'

def formato_fecha(fecha):
    return fecha.strftime('%B ') + str(fecha.day) + sufijo(fecha.day)

def send_email():
    # Configuración del correo
    sender_email = "ecalderon@maxwarehouse.com"
    sender_password = "Maxwarehouse2025$"

    # Lista de destinatarios
    recipient_list = ["ecalderon@maxwarehouse.com", "shippersupport@stamps.com"]

    # Cuerpo del mensaje
    mensaje = f"""Good morning,

    Please provide the Account Transactional Activity Report for {formato_fecha(lunes_pasado)} thru {formato_fecha(domingo_pasado)}.

    Thanks,
    """

    # Crear el mensaje
    subject = "Account Transactional Activity Report"
    msg = MIMEText(mensaje)
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipient_list)

    try:
        # Conectar al servidor SMTP de Outlook
        server = smtplib.SMTP("smtp.office365.com", 587)
        server.starttls()  # Seguridad TLS
        server.login(sender_email, sender_password)  # Autenticación
        server.sendmail(sender_email, recipient_list, msg.as_string())  # Enviar
        server.quit()
        print("✅ Correo enviado correctamente")
    except Exception as e:
        print(f"⚠ Error al enviar el correo: {e}")

# Llamar a la función para enviar el correo
send_email()

