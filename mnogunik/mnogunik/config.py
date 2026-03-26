import socket
import os

# Определяем, где запущен код
hostname = socket.gethostname()
# print(hostname) DESKTOP-USR21ET
# if hostname == 'PaulKrawPC':  # Например, DESKTOP-123ABC
if hostname == 'DESKTOP-USR21ET':  # Например, DESKTOP-123ABC
    # Локальная работа
    ROOT_DIR = 'C:/proj'
    ROOT_URL_OUT = 'http://localhost/outfile'  # если тестируешь через локальный сервер
    ROOT_DIR_OUT = os.path.join(ROOT_DIR, 'outfile')
    nout = True

else:

    ROOT_DIR = '/var/www/mnogunik.ru/proj'
    ROOT_URL_OUT = 'http://mnogunik.ru/outfile'
    ROOT_DIR_OUT = '/var/www/mnogunik.ru/outfile'
    nout = False



    # Работа на сервере paulkraw
    # ROOT_DIR = '/var/www/u1168406/data/www/paulkraw.ru/proj'        
    # ROOT_URL_OUT = 'http://paulkraw.ru/outfile'
    # ROOT_DIR_OUT = '/var/www/u1168406/data/www/paulkraw.ru/outfile'
 # относительный переход к outfile из proj


# ROOT_DIR_OUT = os.path.join(ROOT_DIR, 'outfile')



# # Для локальной работы
# ROOT_DIR = 'C:/proj'

# # Для сервера
# # ROOT_DIR = '/home/paulkraw/public_html'

# ROOT_DIR_OUT = ROOT_DIR + '/outfile'
# ROOT_URL_OUT = 'http://paulkraw.ru/outfile'
