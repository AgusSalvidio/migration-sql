USE [GD2C2020]
GO

-- ********** CREACION SCHEMA  **********

IF(NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'EL_KUELGUE'))
  BEGIN
      exec ('CREATE SCHEMA[EL_KUELGUE]');
      print 'esquema creado';
    END

-- ********** CREACION TABLAS **********

-- CLIENTE
IF OBJECT_ID ('EL_KUELGUE.CLIENTE', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.CLIENTE; 
GO

CREATE TABLE EL_KUELGUE.CLIENTE (
    [CLIENTE_CODIGO] INT IDENTITY(1, 1) PRIMARY KEY,
    [CLIENTE_APELLIDO] [nvarchar](255) NOT NULL,
    [CLIENTE_NOMBRE] [nvarchar](255) NOT NULL,
	[CLIENTE_DIRECCION] [nvarchar](255) NOT NULL,
	[CLIENTE_DNI] [decimal](18, 0) NOT NULL,
	[CLIENTE_FECHA_NAC] [datetime2](3) NOT NULL,
	[CLIENTE_MAIL] [nvarchar](255) NOT NULL
)

-- SUCURSAL
IF OBJECT_ID ('EL_KUELGUE.SUCURSAL', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.SUCURSAL; 
GO

CREATE TABLE EL_KUELGUE.SUCURSAL (
    [SUCURSAL_CODIGO] SMALLINT IDENTITY(1, 1) PRIMARY KEY,
    [SUCURSAL_DIRECCION] [nvarchar](255) NOT NULL,
	[SUCURSAL_MAIL] [nvarchar](255) NOT NULL,
	[SUCURSAL_TELEFONO] [decimal](18, 0) NOT NULL,
	[SUCURSAL_CIUDAD] [nvarchar](255) NOT NULL
)

-- FABRICANTE
IF OBJECT_ID ('EL_KUELGUE.FABRICANTE', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.FABRICANTE; 
GO

CREATE TABLE EL_KUELGUE.FABRICANTE (
    [FABRICANTE_CODIGO] INT IDENTITY(1, 1) PRIMARY KEY,
	[FABRICANTE_NOMBRE] [nvarchar](255) NOT NULL
)

-- MODELO
IF OBJECT_ID ('EL_KUELGUE.MODELO', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.MODELO; 
GO

CREATE TABLE EL_KUELGUE.MODELO (
    [MODELO_CODIGO] [decimal](18, 0) PRIMARY KEY,
    [MODELO_NOMBRE] [nvarchar](255) NOT NULL,
	[MODELO_POTENCIA] [decimal](18, 0) NOT NULL,
	[FABRICANTE_CODIGO] [int] NOT NULL
)

-- TRANSMISION
IF OBJECT_ID ('EL_KUELGUE.TRANSMISION', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.TRANSMISION; 
GO

CREATE TABLE EL_KUELGUE.TRANSMISION (
    [TIPO_TRANSMISION_CODIGO] [decimal](18, 0) PRIMARY KEY,
	[TIPO_TRANSMISION_DESC] [nvarchar](255) NOT NULL
)

-- CAJA
IF OBJECT_ID ('EL_KUELGUE.CAJA', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.CAJA; 
GO

CREATE TABLE EL_KUELGUE.CAJA (
    [TIPO_CAJA_CODIGO] [decimal](18, 0) PRIMARY KEY,
	[TIPO_CAJA_DESC] [nvarchar](255) NOT NULL
)

-- MOTOR
IF OBJECT_ID ('EL_KUELGUE.MOTOR', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.MOTOR; 
GO

CREATE TABLE EL_KUELGUE.MOTOR (
    [TIPO_MOTOR_CODIGO] [decimal](18, 0) PRIMARY KEY
)

-- TIPO_AUTOMOVIL
IF OBJECT_ID ('EL_KUELGUE.TIPO_AUTOMOVIL', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.TIPO_AUTOMOVIL; 
GO

CREATE TABLE EL_KUELGUE.TIPO_AUTOMOVIL (
    [TIPO_AUTO_CODIGO] [decimal](18, 0) PRIMARY KEY,
    [TIPO_AUTO_DESC] [nvarchar](255) NOT NULL
)

-- FICHA_TECNICA_AUTOMOVIL
IF OBJECT_ID ('EL_KUELGUE.FICHA_TECNICA_AUTOMOVIL', 'U') IS NOT NULL
	DROP TABLE EL_KUELGUE.FICHA_TECNICA_AUTOMOVIL
GO

CREATE TABLE EL_KUELGUE.FICHA_TECNICA_AUTOMOVIL (
	[FICHA_TECNICA_CODIGO] INT IDENTITY (1, 1) PRIMARY KEY,
	[TIPO_TRANSMISION_CODIGO] [decimal](18, 0) NOT NULL,
	[TIPO_CAJA_CODIGO] [decimal](18, 0) NOT NULL,
	[TIPO_MOTOR_CODIGO] [decimal](18, 0) NOT NULL,
	[TIPO_AUTO_CODIGO] [decimal](18, 0) NOT NULL,
	[MODELO_CODIGO] [decimal](18, 0) NOT NULL
)

-- AUTOMOVIL
IF OBJECT_ID ('EL_KUELGUE.AUTOMOVIL', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.AUTOMOVIL; 
GO

CREATE TABLE EL_KUELGUE.AUTOMOVIL (
    [AUTO_CODIGO] INT IDENTITY(1, 1) PRIMARY KEY,
	[AUTO_PATENTE] [nvarchar](50) NOT NULL,
	[AUTO_NRO_MOTOR] [nvarchar](50) NOT NULL,
	[AUTO_NRO_CHASIS] [nvarchar](50) NOT NULL,
	[AUTO_FECHA_ALTA] [datetime2](3) NOT NULL,
	[AUTO_CANT_KMS] [decimal](18, 0) NOT NULL,
	[FICHA_TECNICA_CODIGO] [int] NOT NULL
)

-- AUTO_PARTE
IF OBJECT_ID ('EL_KUELGUE.AUTO_PARTE', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.AUTO_PARTE; 
GO

CREATE TABLE EL_KUELGUE.AUTO_PARTE (
    [AUTO_PARTE_CODIGO] [decimal](18, 0) PRIMARY KEY,
	[AUTO_PARTE_DESC] [nvarchar](255) NOT NULL,
	[MODELO_CODIGO] [decimal](18, 0) NOT NULL
)

-- COMPRA
IF OBJECT_ID ('EL_KUELGUE.COMPRA', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.COMPRA; 
GO

CREATE TABLE EL_KUELGUE.COMPRA (
	[COMPRA_NRO] [decimal](18, 0) PRIMARY KEY,
	[COMPRA_FECHA] [datetime2](3) NOT NULL,
	[COMPRA_PRECIO] [decimal](18, 2) NOT NULL,
	[CLIENTE_CODIGO] INT NOT NULL,
	[SUCURSAL_CODIGO] SMALLINT NOT NULL
)

CREATE INDEX INDICE_COMPRA_FECHA ON EL_KUELGUE.COMPRA (COMPRA_FECHA)

-- COMPRA_AUTOMOVIL
IF OBJECT_ID ('EL_KUELGUE.COMPRA_AUTOMOVIL', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.COMPRA_AUTOMOVIL; 
GO

CREATE TABLE EL_KUELGUE.COMPRA_AUTOMOVIL (
	[COMPRA_AUTOMOVIL_CODIGO] INT IDENTITY (1, 1) PRIMARY KEY,
	[COMPRA_NRO] [decimal](18, 0) NOT NULL,
	[AUTO_CODIGO] INT NOT NULL
)

-- DETALLE_COMPRA_AUTO_PARTE
IF OBJECT_ID ('EL_KUELGUE.DETALLE_COMPRA_AUTO_PARTE', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.DETALLE_COMPRA_AUTO_PARTE; 
GO

CREATE TABLE EL_KUELGUE.DETALLE_COMPRA_AUTO_PARTE (
	[DETALLE_CODIGO] INT IDENTITY(1, 1) PRIMARY KEY,
	[COMPRA_NRO] [decimal](18, 0) NOT NULL,
	[AUTO_PARTE_CODIGO] [decimal](18, 0) NOT NULL,
	[COMPRA_CANT] [decimal](18, 0) NOT NULL,
	[PRECIO_UNITARIO] [decimal](18, 2) NOT NULL
)

-- FACTURA
IF OBJECT_ID ('EL_KUELGUE.FACTURA', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.FACTURA; 
GO

CREATE TABLE EL_KUELGUE.FACTURA (
	[FACTURA_NRO] [decimal](18, 0) PRIMARY KEY,
	[FACTURA_FECHA] [datetime2](3) NOT NULL,
	[FACTURA_PRECIO] [decimal](18, 2) NOT NULL,
	[CLIENTE_CODIGO] INT NOT NULL,
	[SUCURSAL_CODIGO] SMALLINT NOT NULL
)

CREATE INDEX INDICE_FACTURA_FECHA ON EL_KUELGUE.FACTURA (FACTURA_FECHA)

-- FACTURA_AUTOMOVIL
IF OBJECT_ID ('EL_KUELGUE.FACTURA_AUTOMOVIL', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.FACTURA_AUTOMOVIL; 
GO

CREATE TABLE EL_KUELGUE.FACTURA_AUTOMOVIL (
	[FACTURA_AUTOMOVIL_CODIGO] INT IDENTITY (1, 1) PRIMARY KEY,
	[FACTURA_NRO] [decimal](18, 0) NOT NULL,
	[AUTO_CODIGO] INT NOT NULL
)

-- DETALLE_FACTURA_AUTO_PARTE
IF OBJECT_ID ('EL_KUELGUE.DETALLE_FACTURA_AUTO_PARTE', 'U') IS NOT NULL  
   DROP TABLE EL_KUELGUE.DETALLE_FACTURA_AUTO_PARTE; 
GO

CREATE TABLE EL_KUELGUE.DETALLE_FACTURA_AUTO_PARTE (
	[DETALLE_CODIGO] INT IDENTITY(1, 1) PRIMARY KEY,
	[FACTURA_NRO] [decimal](18, 0) NOT NULL,
	[AUTO_PARTE_CODIGO] [decimal](18, 0) NOT NULL,
	[FACTURA_CANT] [decimal](18, 0) NOT NULL,
	[PRECIO_UNITARIO] [decimal](18, 2) NOT NULL
)


-- ********** MIGRACION **********


-- MIGRACION CLIENTE
IF OBJECT_ID ('EL_KUELGUE.migracionCLIENTE', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionCLIENTE; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionCLIENTE AS
BEGIN
	INSERT INTO EL_KUELGUE.CLIENTE
	SELECT DISTINCT CLIENTE_APELLIDO, CLIENTE_NOMBRE, CLIENTE_DIRECCION, CLIENTE_DNI, CLIENTE_FECHA_NAC, CLIENTE_MAIL
	FROM gd_esquema.Maestra
	WHERE CLIENTE_APELLIDO IS NOT NULL
	UNION
	SELECT DISTINCT FAC_CLIENTE_APELLIDO, FAC_CLIENTE_NOMBRE, FAC_CLIENTE_DIRECCION, FAC_CLIENTE_DNI, FAC_CLIENTE_FECHA_NAC, FAC_CLIENTE_MAIL
	FROM gd_esquema.Maestra
	WHERE FAC_CLIENTE_APELLIDO IS NOT NULL
END;
GO

-- MIGRACION SUCURSAL
-- Desprecio las sucursales FAC_SUCURSAL_* puesto a que son las mismas 7 que se obtienen con esta query
IF OBJECT_ID ('EL_KUELGUE.migracionSUCURSAL', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionSUCURSAL; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionSUCURSAL AS
BEGIN
	INSERT INTO EL_KUELGUE.SUCURSAL
	SELECT DISTINCT SUCURSAL_DIRECCION, SUCURSAL_MAIL, SUCURSAL_TELEFONO, SUCURSAL_CIUDAD
	FROM gd_esquema.Maestra
	WHERE SUCURSAL_CIUDAD IS NOT NULL
END;
GO

-- MIGRACION FABRICANTE
IF OBJECT_ID ('EL_KUELGUE.migracionFABRICANTE', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionFABRICANTE; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionFABRICANTE AS
BEGIN
	INSERT INTO EL_KUELGUE.FABRICANTE
	SELECT DISTINCT [FABRICANTE_NOMBRE]
	FROM gd_esquema.Maestra
	WHERE [FABRICANTE_NOMBRE] IS NOT NULL
END;
GO

-- MIGRACION MODELO
IF OBJECT_ID ('EL_KUELGUE.migracionMODELO', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionMODELO; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionMODELO AS
BEGIN
	INSERT INTO EL_KUELGUE.MODELO
	SELECT DISTINCT MODELO_CODIGO, MODELO_NOMBRE, MODELO_POTENCIA, FABRICANTE_CODIGO
	FROM gd_esquema.Maestra m JOIN EL_KUELGUE.FABRICANTE f ON f.FABRICANTE_NOMBRE = m.FABRICANTE_NOMBRE 
	WHERE[MODELO_CODIGO] IS NOT NULL
END;
GO
-- MIGRACION TRANSMISION
IF OBJECT_ID ('EL_KUELGUE.migracionTRANSMISION', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionTRANSMISION; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionTRANSMISION AS
BEGIN
	INSERT INTO EL_KUELGUE.TRANSMISION
	SELECT DISTINCT TIPO_TRANSMISION_CODIGO, TIPO_TRANSMISION_DESC
	FROM gd_esquema.Maestra
	WHERE TIPO_TRANSMISION_CODIGO IS NOT NULL
END;
GO

-- MIGRACION CAJA
IF OBJECT_ID ('EL_KUELGUE.migracionCAJA', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionCAJA; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionCAJA AS
BEGIN
	INSERT INTO EL_KUELGUE.CAJA
	SELECT DISTINCT TIPO_CAJA_CODIGO, TIPO_CAJA_DESC
	FROM gd_esquema.Maestra
	WHERE TIPO_CAJA_CODIGO IS NOT NULL
END;
GO

-- MIGRACION MOTOR
IF OBJECT_ID ('EL_KUELGUE.migracionMOTOR', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionMOTOR; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionMOTOR AS
BEGIN
	INSERT INTO EL_KUELGUE.MOTOR
	SELECT DISTINCT TIPO_MOTOR_CODIGO
	FROM gd_esquema.Maestra
	WHERE TIPO_MOTOR_CODIGO IS NOT NULL
END;
GO

-- MIGRACION TIPO_AUTOMOVIL
IF OBJECT_ID ('EL_KUELGUE.migracionTIPO_AUTOMOVIL', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionTIPO_AUTOMOVIL; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionTIPO_AUTOMOVIL AS
BEGIN
	INSERT INTO EL_KUELGUE.TIPO_AUTOMOVIL
	SELECT DISTINCT TIPO_AUTO_CODIGO, TIPO_AUTO_DESC
	FROM gd_esquema.Maestra
	WHERE TIPO_AUTO_CODIGO IS NOT NULL
END;
GO

-- MIGRACION FICHA_TECNICA_AUTOMOVIL
IF OBJECT_ID ('EL_KUELGUE.migracionFICHA_TECNICA_AUTOMOVIL', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionFICHA_TECNICA_AUTOMOVIL; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionFICHA_TECNICA_AUTOMOVIL AS
BEGIN
	INSERT INTO EL_KUELGUE.FICHA_TECNICA_AUTOMOVIL
	SELECT DISTINCT TIPO_TRANSMISION_CODIGO, TIPO_CAJA_CODIGO, TIPO_MOTOR_CODIGO, TIPO_AUTO_CODIGO, MODELO_CODIGO
	FROM gd_esquema.Maestra
	WHERE TIPO_TRANSMISION_CODIGO IS NOT NULL
END;
GO

-- MIGRACION AUTOMOVIL
IF OBJECT_ID ('EL_KUELGUE.migracionAUTOMOVIL', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionAUTOMOVIL; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionAUTOMOVIL AS
BEGIN
	INSERT INTO EL_KUELGUE.AUTOMOVIL
	SELECT DISTINCT AUTO_PATENTE, AUTO_NRO_MOTOR, AUTO_NRO_CHASIS, AUTO_FECHA_ALTA, AUTO_CANT_KMS, FICHA_TECNICA_CODIGO
	FROM gd_esquema.Maestra m JOIN EL_KUELGUE.FICHA_TECNICA_AUTOMOVIL f ON (m.MODELO_CODIGO = f.MODELO_CODIGO)
	WHERE AUTO_PATENTE IS NOT NULL
END;
GO

-- MIGRACION AUTO_PARTE
IF OBJECT_ID ('EL_KUELGUE.migracionAUTO_PARTE', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionAUTO_PARTE; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionAUTO_PARTE AS
BEGIN
	INSERT INTO EL_KUELGUE.AUTO_PARTE
	SELECT DISTINCT AUTO_PARTE_CODIGO, AUTO_PARTE_DESCRIPCION, MODELO_CODIGO
	FROM gd_esquema.Maestra
	WHERE AUTO_PARTE_CODIGO IS NOT NULL
END;
GO

-- MIGRACION COMPRA
IF OBJECT_ID ('EL_KUELGUE.migracionCOMPRA', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionCOMPRA; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionCOMPRA AS
BEGIN
	INSERT INTO EL_KUELGUE.COMPRA
	SELECT DISTINCT COMPRA_NRO, COMPRA_FECHA, COMPRA_PRECIO, CLIENTE_CODIGO, SUCURSAL_CODIGO
	FROM gd_esquema.Maestra m JOIN EL_KUELGUE.CLIENTE c ON (m.CLIENTE_APELLIDO = c.CLIENTE_APELLIDO AND m.CLIENTE_NOMBRE = c.CLIENTE_NOMBRE AND m.CLIENTE_DNI = c.CLIENTE_DNI)
							  JOIN EL_KUELGUE.SUCURSAL s ON (m.SUCURSAL_TELEFONO = s.SUCURSAL_TELEFONO)
	WHERE COMPRA_NRO IS NOT NULL AND FACTURA_NRO IS NULL AND AUTO_PARTE_CODIGO IS NULL
	UNION
	SELECT DISTINCT COMPRA_NRO, COMPRA_FECHA, SUM(COMPRA_PRECIO) COMPRA_PRECIO, CLIENTE_CODIGO, SUCURSAL_CODIGO
	FROM gd_esquema.Maestra m JOIN EL_KUELGUE.CLIENTE c ON (m.CLIENTE_APELLIDO = c.CLIENTE_APELLIDO AND m.CLIENTE_NOMBRE = c.CLIENTE_NOMBRE AND m.CLIENTE_DNI = c.CLIENTE_DNI)
							  JOIN EL_KUELGUE.SUCURSAL s ON (m.SUCURSAL_TELEFONO = s.SUCURSAL_TELEFONO)
	WHERE COMPRA_NRO IS NOT NULL AND FACTURA_NRO IS NULL AND AUTO_PARTE_CODIGO IS NOT NULL
	GROUP BY COMPRA_NRO, COMPRA_FECHA, CLIENTE_CODIGO, SUCURSAL_CODIGO
END;
GO

-- MIGRACION COMPRA_AUTOMOVIL
IF OBJECT_ID ('EL_KUELGUE.migracionCOMPRA_AUTOMOVIL', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionCOMPRA_AUTOMOVIL; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionCOMPRA_AUTOMOVIL AS
BEGIN
	INSERT INTO EL_KUELGUE.COMPRA_AUTOMOVIL
	SELECT DISTINCT COMPRA_NRO, AUTO_CODIGO
	FROM gd_esquema.Maestra m JOIN EL_KUELGUE.AUTOMOVIL a ON (m.AUTO_NRO_CHASIS = a.AUTO_NRO_CHASIS)
	WHERE COMPRA_NRO IS NOT NULL AND FACTURA_NRO IS NULL AND AUTO_PARTE_CODIGO IS NULL
END;
GO

-- MIGRACION DETALLE_COMPRA_AUTO_PARTE
IF OBJECT_ID ('EL_KUELGUE.migracionDETALLE_COMPRA_AUTO_PARTE', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionDETALLE_COMPRA_AUTO_PARTE; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionDETALLE_COMPRA_AUTO_PARTE AS
BEGIN
	INSERT INTO EL_KUELGUE.DETALLE_COMPRA_AUTO_PARTE
	SELECT DISTINCT COMPRA_NRO, AUTO_PARTE_CODIGO, SUM(COMPRA_CANT), COMPRA_PRECIO
	FROM gd_esquema.Maestra
	WHERE COMPRA_NRO IS NOT NULL AND FACTURA_NRO IS NULL AND AUTO_PARTE_CODIGO IS NOT NULL
	GROUP BY COMPRA_NRO, AUTO_PARTE_CODIGO, COMPRA_PRECIO
END;
GO

-- MIGRACION FACTURA
IF OBJECT_ID ('EL_KUELGUE.migracionFACTURA', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionFACTURA; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionFACTURA AS
BEGIN
	INSERT INTO EL_KUELGUE.FACTURA
	SELECT DISTINCT FACTURA_NRO, FACTURA_FECHA, PRECIO_FACTURADO, CLIENTE_CODIGO, SUCURSAL_CODIGO
	FROM gd_esquema.Maestra m JOIN EL_KUELGUE.CLIENTE c ON (m.FAC_CLIENTE_APELLIDO = c.CLIENTE_APELLIDO AND m.FAC_CLIENTE_NOMBRE = c.CLIENTE_NOMBRE AND m.FAC_CLIENTE_DNI = c.CLIENTE_DNI)
							  JOIN EL_KUELGUE.SUCURSAL s ON (m.FAC_SUCURSAL_TELEFONO = s.SUCURSAL_TELEFONO)
	WHERE FACTURA_NRO IS NOT NULL AND AUTO_PARTE_CODIGO IS NULL
	UNION
	SELECT DISTINCT FACTURA_NRO, FACTURA_FECHA, SUM(PRECIO_FACTURADO) PRECIO_FACTURADO, CLIENTE_CODIGO, SUCURSAL_CODIGO
	FROM gd_esquema.Maestra m JOIN EL_KUELGUE.CLIENTE c ON (m.FAC_CLIENTE_APELLIDO = c.CLIENTE_APELLIDO AND m.FAC_CLIENTE_NOMBRE = c.CLIENTE_NOMBRE AND m.FAC_CLIENTE_DNI = c.CLIENTE_DNI)
							  JOIN EL_KUELGUE.SUCURSAL s ON (m.FAC_SUCURSAL_TELEFONO = s.SUCURSAL_TELEFONO)
	WHERE COMPRA_NRO IS NULL AND FACTURA_NRO IS NOT NULL AND AUTO_PARTE_CODIGO IS NOT NULL
	GROUP BY FACTURA_NRO, FACTURA_FECHA, CLIENTE_CODIGO, SUCURSAL_CODIGO
END;
GO

-- MIGRACION FACTURA_AUTOMOVIL
IF OBJECT_ID ('EL_KUELGUE.migracionFACTURA_AUTOMOVIL', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionFACTURA_AUTOMOVIL; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionFACTURA_AUTOMOVIL AS
BEGIN
	INSERT INTO EL_KUELGUE.FACTURA_AUTOMOVIL
	SELECT DISTINCT FACTURA_NRO, AUTO_CODIGO
	FROM gd_esquema.Maestra m JOIN EL_KUELGUE.AUTOMOVIL a ON (m.AUTO_NRO_CHASIS = a.AUTO_NRO_CHASIS)
	WHERE FACTURA_NRO IS NOT NULL AND AUTO_PARTE_CODIGO IS NULL
END;
GO

-- MIGRACION DETALLE_FACTURA_AUTO_PARTE
IF OBJECT_ID ('EL_KUELGUE.migracionDETALLE_FACTURA_AUTO_PARTE', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migracionDETALLE_FACTURA_AUTO_PARTE; 
GO
CREATE PROCEDURE EL_KUELGUE.migracionDETALLE_FACTURA_AUTO_PARTE AS
BEGIN
	INSERT INTO EL_KUELGUE.DETALLE_FACTURA_AUTO_PARTE
	SELECT DISTINCT FACTURA_NRO, AUTO_PARTE_CODIGO, SUM(CANT_FACTURADA), PRECIO_FACTURADO
	FROM gd_esquema.Maestra
	WHERE COMPRA_NRO IS NULL AND FACTURA_NRO IS NOT NULL AND AUTO_PARTE_CODIGO IS NOT NULL
	GROUP BY FACTURA_NRO, AUTO_PARTE_CODIGO, COMPRA_PRECIO, PRECIO_FACTURADO
END;
GO

--MIGRAR TODA LA BD
IF OBJECT_ID ('EL_KUELGUE.migrarBD', 'P') IS NOT NULL  
   DROP PROCEDURE EL_KUELGUE.migrarBD; 
GO
CREATE PROCEDURE EL_KUELGUE.migrarBD AS
BEGIN
	EXEC EL_KUELGUE.migracionCLIENTE
	EXEC EL_KUELGUE.migracionSUCURSAL
	EXEC EL_KUELGUE.migracionFABRICANTE
	EXEC EL_KUELGUE.migracionMODELO
	EXEC EL_KUELGUE.migracionTRANSMISION
	EXEC EL_KUELGUE.migracionCAJA
	EXEC EL_KUELGUE.migracionMOTOR
	EXEC EL_KUELGUE.migracionTIPO_AUTOMOVIL
	EXEC EL_KUELGUE.migracionFICHA_TECNICA_AUTOMOVIL
	EXEC EL_KUELGUE.migracionAUTOMOVIL
	EXEC EL_KUELGUE.migracionAUTO_PARTE
	EXEC EL_KUELGUE.migracionCOMPRA
	EXEC EL_KUELGUE.migracionCOMPRA_AUTOMOVIL
	EXEC EL_KUELGUE.migracionDETALLE_COMPRA_AUTO_PARTE
	EXEC EL_KUELGUE.migracionFACTURA
	EXEC EL_KUELGUE.migracionFACTURA_AUTOMOVIL
	EXEC EL_KUELGUE.migracionDETALLE_FACTURA_AUTO_PARTE
END
GO

exec EL_KUELGUE.migrarBD


-- ********** CREACION CONSTRAINS  **********

--FK MODELO
ALTER TABLE EL_KUELGUE.MODELO
ADD FOREIGN KEY ([FABRICANTE_CODIGO]) REFERENCES EL_KUELGUE.FABRICANTE([FABRICANTE_CODIGO])

--FK FICHA_TECNICA_AUTOMOVIL
ALTER TABLE EL_KUELGUE.FICHA_TECNICA_AUTOMOVIL
ADD 
	FOREIGN KEY ([TIPO_TRANSMISION_CODIGO]) REFERENCES EL_KUELGUE.TRANSMISION([TIPO_TRANSMISION_CODIGO]),
	FOREIGN KEY ([TIPO_CAJA_CODIGO]) REFERENCES EL_KUELGUE.CAJA([TIPO_CAJA_CODIGO]),
	FOREIGN KEY ([TIPO_MOTOR_CODIGO]) REFERENCES EL_KUELGUE.MOTOR([TIPO_MOTOR_CODIGO]),
	FOREIGN KEY ([TIPO_AUTO_CODIGO]) REFERENCES EL_KUELGUE.TIPO_AUTOMOVIL([TIPO_AUTO_CODIGO]),
	FOREIGN KEY ([MODELO_CODIGO]) REFERENCES EL_KUELGUE.MODELO([MODELO_CODIGO]);
GO

--FK AUTOMOVIL
ALTER TABLE EL_KUELGUE.AUTOMOVIL
ADD FOREIGN KEY ([FICHA_TECNICA_CODIGO]) REFERENCES EL_KUELGUE.FICHA_TECNICA_AUTOMOVIL([FICHA_TECNICA_CODIGO])

--FK AUTO_PARTE
ALTER TABLE EL_KUELGUE.AUTO_PARTE
ADD FOREIGN KEY ([MODELO_CODIGO]) REFERENCES EL_KUELGUE.MODELO([MODELO_CODIGO])

--FK COMPRA
ALTER TABLE EL_KUELGUE.COMPRA
ADD
	FOREIGN KEY ([CLIENTE_CODIGO]) REFERENCES EL_KUELGUE.CLIENTE([CLIENTE_CODIGO]),
	FOREIGN KEY ([SUCURSAL_CODIGO]) REFERENCES EL_KUELGUE.SUCURSAL([SUCURSAL_CODIGO])
GO

--FK COMPRA_AUTOMOVIL
ALTER TABLE EL_KUELGUE.COMPRA_AUTOMOVIL
ADD
	FOREIGN KEY ([COMPRA_NRO]) REFERENCES EL_KUELGUE.COMPRA([COMPRA_NRO]),
	FOREIGN KEY ([AUTO_CODIGO]) REFERENCES EL_KUELGUE.AUTOMOVIL([AUTO_CODIGO])
GO

--FK DETALLE_COMPRA_AUTO_PARTE
ALTER TABLE EL_KUELGUE.DETALLE_COMPRA_AUTO_PARTE
ADD
	FOREIGN KEY ([COMPRA_NRO]) REFERENCES EL_KUELGUE.COMPRA([COMPRA_NRO]),
	FOREIGN KEY ([AUTO_PARTE_CODIGO]) REFERENCES EL_KUELGUE.AUTO_PARTE([AUTO_PARTE_CODIGO])
GO

--FK FACTURA
ALTER TABLE EL_KUELGUE.FACTURA
ADD
	FOREIGN KEY ([CLIENTE_CODIGO]) REFERENCES EL_KUELGUE.CLIENTE([CLIENTE_CODIGO]),
	FOREIGN KEY ([SUCURSAL_CODIGO]) REFERENCES EL_KUELGUE.SUCURSAL([SUCURSAL_CODIGO])
GO

--FK FACTURA_AUTOMOVIL
ALTER TABLE EL_KUELGUE.FACTURA_AUTOMOVIL
ADD
	FOREIGN KEY ([FACTURA_NRO]) REFERENCES EL_KUELGUE.FACTURA([FACTURA_NRO]),
	FOREIGN KEY ([AUTO_CODIGO]) REFERENCES EL_KUELGUE.AUTOMOVIL([AUTO_CODIGO])
GO

--FK DETALLE_FACTURA_AUTO_PARTE
ALTER TABLE EL_KUELGUE.DETALLE_FACTURA_AUTO_PARTE
ADD
	FOREIGN KEY ([FACTURA_NRO]) REFERENCES EL_KUELGUE.FACTURA([FACTURA_NRO]),
	FOREIGN KEY ([AUTO_PARTE_CODIGO]) REFERENCES EL_KUELGUE.AUTO_PARTE([AUTO_PARTE_CODIGO])
GO
