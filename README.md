# Fabric Accelerator
Fabric Accelerator es una solución desarrollada en Microsoft Fabric que permite implementar de manera rápida y sencilla la primera fase de una arquitectura de datos en medallón. Su objetivo principal es acelerar el inicio de proyectos de ingeniería de datos mediante un conjunto de configuraciones guiadas que automatizan la ingesta y la creación de la **capa Bronze**.

**Beneficios principales**
*   **Rapidez**: en pocos pasos de configuración, se habilita un flujo de ingesta estandarizado.
    
*   **Simplicidad**: no requiere desarrollos complejos, está basado en parametrización y buenas prácticas.
    
*   **Estandarización**: garantiza consistencia en la capa Bronze para cualquier origen de datos.
    
*   **Escalabilidad**: preparado para extenderse hacia Silver y Gold siguiendo la arquitectura en medallón.
    
*   **Multiplataforma**: aunque nace en Microsoft Fabric, el concepto es aplicable a otras plataformas de datos modernas como **Azure Synapse Analytics** o **Databricks**, gracias a que se fundamenta en patrones y prácticas de arquitectura más que en dependencias técnicas específicas.
    
**Alcance funcional**
*   Ingesta de datos desde múltiples orígenes soportados por Fabric.
    
*   Automatización de la escritura en la capa Bronze (formato Delta).
    
*   Configuración flexible mediante metadatos.
    
*   Base para aplicar controles de calidad y gobierno de datos desde etapas iniciales.
    
**Valor para la organización**  
Con Fabric Accelerator, las organizaciones pueden poner en marcha su plataforma de datos en Fabric de forma inmediata, reduciendo el tiempo de arranque de semanas a horas, y asegurando que el diseño inicial sigue los estándares modernos de ingeniería de datos en la nube.  
Además, al estar basado en un **enfoque arquitectónico replicable**, se convierte en un marco conceptual que también puede implementarse en entornos **Synapse** o **Databricks**, ofreciendo continuidad y consistencia en ecosistemas de datos híbridos.

#Solución Técnica
En esta versión de una solución **Fabric Accelerator** se intentó adaptar la propuesta de [bennyaustin](https://github.com/bennyaustin/fabric-accelerator) a otras experiencias en  soluciones de **integraciones de datos** Tomando lo más conveniente de ambas según mi experiencia.

* * *

**Objetivo**
------------------------

Esta solución implementa un flujo de ingesta y procesamiento de datos en Microsoft Fabric, orquestado por un sistema basado en metadatos para ejecutar notebooks de forma dinámica.  

El objetivo es integrar datos desde múltiples orígenes en una arquitectura **Medallion** (Bronze, Silver, Gold), optimizando la reutilización y minimizando el mantenimiento.
* * *
