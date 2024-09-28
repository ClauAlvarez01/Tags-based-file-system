# Tag based - File System

El proyecto consiste en un sistema distribuido para el almacenamiento y gestión de archivos basados en tags. Un **tag** puede etiquetar múltiples archivos, y un archivo puede tener múltiples tags. La distribución del sistema está basada en **Chord**.



## Ejecución de desarrollo:

A continuación se muestran los pasos necesarios para poder ejecutar la aplicación en desarrollo.

### Requisitios iniciales:

1. Abrir **Docker Desktop** para que se inicie el servidor de Docker.
2. Tener descargada y disponible alguna **imagen de python** en Docker.

### Servidor distribuido:

1. Abrir una terminal interactiva de la imagen de python, utilizando como volumen el directorio del proyecto.

   Ejecutando el siguiente comando en una terminal:

   ```bash
   docker run --rm -it -v <path_>:/app <python_img> /bin/bash
   ```

   - `<path_>`: la ruta completa de ubicación del proyecto. (Ej: C:\Documents\GitHub\Tags-based-file-system)
   - `<python_img>`: la imagen de python descargada y su versión. (Ej: python:3)

2. Una vez abierta la terminal interactiva de linux, vamos al directorio `/app`.

   ```bash
   cd app
   ```

3. Creamos un primer nodo de Chord. De esta forma tendríamos un anillo donde solo hay un nodo.

   ```bash
   python QueryNode.py
   ```

4. Creamos el segundo nodo repitiendo los pasos anteriores a partir del 3. Pero esta vez el nodo se crea (en sustitución al paso 5) utilizando el flag: `-c`

   Conociendo la IP del nodo al que queremos conectarnos:

   ```bash
   python QueryNode.py -c 172.17.0.2
   ```

   O con autodescubrimiento:

   ```bash
   python QueryNode.py -c
   ```

5. De esta misma forma se pueden incluir la cantidad de nodos deseados a la red de chord.

### Aplicación cliente:

1. Abrir una terminal interactiva de la imagen de python, utilizando como volumen el directorio del proyecto.

   ```bash
   docker run --rm -it -v <path_>:/app <python_img> /bin/bash
   ```

2. Una vez abierta la terminal interactiva de linux, vamos al directorio `/app/client`.

   ```bash
   cd app/client
   ```

3. Ejecutar la aplicación de consola `client.py`.

   ```bash
   python client.py
   ```





## User Interface (UI):

La UI consiste en una aplicación de consola, a través de la que se interactúa con el sistema distribuido mediante comandos. 

```ini
=COMMANDS===================================
=> add           <file-list>  <tag-list>  <=
=> delete        <tag-query>              <=
=> list          <tag-query>              <=
=> add-tags      <tag-query>  <tag-list>  <=
=> delete-tags   <tag-query>  <tag-list>  <=
=> download      <tag-query>              <=
=> inspect-tag   <tag>                    <=
=> inspect-file  <file-name>              <=
=> info                                   <=
=> exit                                   <=
============================================
⚠ Use (;) separator for lists of elements
example  <tag-list>  as  red;blue
⚠  green text means succeded
⚠  red text   means failed
============================================
```

### Arquitectura:

No es más que un servidor que escucha peticiones del usuario y las redirige al sistema distribuido, mostrando la respuesta obtenida de este. Su implementación está contenida en el directorio `client`.  Cuando se ejecuta la aplicación, hay dos posibilidades de enlazar con el servidor distribuido, mediante la dirección IP de algún nodo de la red distribuida, o mediante autodescubrimiento. 

En el directorio `client/resources` se debe tener todo archivo que se desee subir al sistema. Además en `client/downloads` se encuentran los archivos descargados por el usuario.

Si alguna vez el nodo del sistema distribuido con el que se establece la conexión se cae, el cliente es capaz de automaticamente reanudar la conexión con el sistema.

Todo comando que se ejecute pasa por los siguientes pasos:

1. **Identificación de partes**: se separa el comando y sus parámetros.
2. **Validación sintáctica**: se comprueba la correctitud del formato de entrada del comando y parámetros.
3. **Identificación del comando**: se diferencia de entre el resto.
4. **Solicitud de operación**: se envía un mensaje al servidor solicitando permiso para realizar tal comando.
5. **Envío de información**: tras recibir la confirmación del permiso, se envía toda la información necesaria para ejecutar tal solicitud en el sistema.
6. **Recepción y visualización**: se espera la respuesta del servidor y se muestran los resultados en pantalla al usuario.

### Interacción:

#### ¿Cómo se ejecuta?

Conectando a una IP específica:

```bash
python client.py 172.17.0.2
```

Conectando a cualquier nodo del sistema:

```bash
python client.py
```

#### ¿Qué funcionalidades tiene?

##### Veamos antes que son los parámetros:

- `<file-list>`: Una lista de archivos, representados por sus nombres. Se deben escribir sin espacios y utilizando el símbolo `;` como separador. Por ejemplo, si tenemos dos archivos llamados **file1.txt** y **file2.py**: `file1.txt;file2.py`. Los archivos asociados a esos nombres deben encontrarse en el directorio `client/resources`. 
- `<tag-list>`: Un conjunto de tags en forma de string. No pueden contener espacios, y deben separarse por `;`. Por ejemplo si tenemos las etiquetas **red** y **blue**: `red;blue`.
- `<tag-query>`: Un conjunto de tags en forma de string, que constituyen una consulta. La respuesta a esa consulta es el conjunto de todos los archivos del sistema, tales que en su conjunto de tags, están las tags de la query. Por ejemplo si la tag-query es `red;blue`, la respuesta a esa query es `file1.txt`, suponiendo que ese archivo sea el único que tiene tanto a **red** como a **blue** en su conjunto de tags.
- `<tag>`: El nombre de una tag en forma de string. Ejemplo: `red`.
- `<file-name>`: El nombre de un archivo. Ejemplo: `file2.py`.

##### Funcionalidades:

- `add <file-list> <tag-list>`: Añade una lista de archivos al sistema de archivos, de forma que todos se inserten con todos los tags contenidos en la lista de tags.
- `delete <tag-query>`: Borra del sistema todos los archivos que cumplan la query.
- `list <tag-query>`: Muestra una lista de todos los archivos que cumplen la query, incluyendo la lista de todos los tags que tiene cada uno.
- `add-tags <tag-query> <tag-list>`: Añade las tags de la lista de tags dada, a todos los archivos que cumplan la query.
- `delete-tags <tag-query> <tag-list>`: Borra las tags en la lista de tags dada, de todos los archivos que cumplan la query.
- `download <tag-query>`: Descarga del sistema todos los archivos que cumplen la query. Se almacenan en el directorio `/client/downloads`.
- `inspect-tag <tag>`: Muestra el nombre de los archivos que contienen el tag dado.
- `inspect-file <file-name>`: Muestra todas las tags que tiene el archivo con un nombre dado.
- `info`: Muestra la lista de comandos que ofrecen funcionalidades (*lista actual*).
- `exit`: Termina la ejecución del cliente.





## Servidor distribuido:

### Arquitectura

La arquitectura adoptada para el sistema distribuido es una arquitectura mediante capas. Y la topología que sigue la red es la de **Chord**. Un nodo del sistema está compuesto por 3 capas fundamentales: **Capa de Chord**, **Capa de Datos** y la **Capa de Consulta**. Estas capas tienen funciones específicas cada una, a continuación las veremos con más detenimiento.

**Capa de Chord**: es la capa de más bajo nivel, y su función en la de tener implementada todas las funcionalidades necesarias para asegurar la comunicación esencial entre los nodos, con el fin de mantener la topología de la red de Chord. Está implementada en `ChordNode.py`.

**Capa de Datos**: se sitúa en un nivel medio, y se encarga de manejar todos los datos del sistema distribuido, almacenamiento y consulta. Esta también tiene interacción con los otros nodos del sistema, al nivel de capa de datos. Está implementada en `DataNode.py`.

**Capa de Consulta**: es la capa más exterior de un nodo, y es la encargada de recibir las consultas de los clientes, interactuar con la capa inferior para satisfacer las consultas y ofrecer una respuesta al cliente sobre su solicitud. Está implementada en `QueryNode.py`.

La comunicación entre los nodos del sistema es *peer to peer*, mediante sockets de python.

Existe un nodo en el sistema distribuido que actúa como el **líder**. Inicialmente el líder es el primer nodo que forma el anillo de Chord, y en caso de que este salga del sistema, la elección del líder se realiza de forma **bully**. La noción del líder es importante en nuestro sistema, pues este será el encargado de manejar y velar por el mantenimiento de la **sincronización y exclusión mutua**, cuando múltiples clientes hacen solicitudes simultáneas sobre un mismo dato.

El sistema distribuido maneja la réplica de la información, de forma que se evite la pérdida de datos si el nodo responsable sale del sistema.

### Pruebas y depuración

En el directorio `/database` se encuentra el almacenamiento de los nodos del anillo. Cada nodo tiene un directorio nombrado con su dirección ip. Dentro de cada uno se pueden ver los datos que este almacena. Esta arquitectura de almacenamiento se adoptó por la facilidad que ofrece para pruebas y depuración.

Además tenemos el directorio `/logs`, donde se guarda un archivo de texto por cada nodo, identificado con su dirección ip. Cada archivo de texto contiene de forma resumida la información clave que está almacenanda en cada nodo del sistema.