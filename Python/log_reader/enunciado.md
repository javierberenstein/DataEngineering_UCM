# Análisis de fichero de acceso.

En teoría hemos analizado el ficero de accesos a fichero log. Vamos
ahora a realizar un ejercicio para afianzar conceptos de listas,
diccionarios, conjuntos y expresiones regulares. Haz un programa con
las siguientes funciones:

- `ipaddreses(filename: str) -> set[str]`, que devielve un conjunto con
  las direcciones IP que han accedido al servidor que no sean
  *bots*. Una de las cadenas de caracteres que aparece es el navegador
  que accede a la página. En la sugiente línea se identifica el
  navegador Mozilla desde un sistema Linux/Ubuntu.

147.96.46.52 - - [10/Oct/2023:12:55:47 +0200] "GET /favicon.ico HTTP/1.1" 404 519 "https://antares.sip.ucm.es/" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0"


  Los *bots* se idenditifican porque pone algo de bot o Bot en se
  identificador. Por ejemplo

213.180.203.109 - - [15/Sep/2023:00:12:18 +0200] "GET /robots.txt HTTP/1.1" 302 567 "-" "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)"

66.249.66.35 - - [15/Sep/2023:00:18:46 +0200] "GET /~luis/sw05-06/libre_m2_baja.pdf HTTP/1.1" 200 5940849 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.goog
le.com/bot.html)"

- Para la realización de esa función se debe realizar las  funcines
  * `get_user_agent(line: str) ->str`, que devuelve el agente de
    usuario de la línea.
  * `is_bot(line: str) -> bool`. Que indica que el acceso indicado es
	esa línea es un bot o no.


- `histbyhour(filename: str) -> dict[int, int]`. El histograma por hora
  de acceso por horas, es decir, el número de accesos que ha habido en
  cada hora.

  Para la realización de esta función es necesario realizar la función
  `get_hour(line: str) -> int` que devuelva la hora en la que se ha
  realizado el aceso.


En todas funciones hay que poner correctamente los docstring. Se deben
indicar casos de prueba `doctests`.

**NOTA**: Para probar las funciones de los ficheros es mejor usar
ficheros de unas pocas líneas en las que podamos probar el correcto
funcionamiento.
