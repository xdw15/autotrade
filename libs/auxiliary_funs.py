from sit_toolbox import query_sql
import polars as pl


def sit_qs(lafecha):
    """
    nothing much to say
    :param lafecha:
    :return:
    """
    # lafecha = 20250122
    aver = query_sql(
        f"""
        select
        ctv.Cantidad,
        ctv.CodigoPortafolioSBS as Portafolio,
        ctv.CodigoMnemonico as Name,
        v.CodigoISIN as ISIN,
        v.CodigoSBS,
        v.TipoRenta,
        v.ValorUnitario,
        ctv.ValorCausadoOriginal,
        ctv.ValorCausadoLocal,
        ctv.VPNOriginal,
        ctv.VPNLocal,
        v.CodigoMoneda as Moneda,
        v.FechaVencimiento
    
        from CarteraTituloValoracion ctv
        left join valores v 
        on ctv.CodigoMnemonico = v.CodigoNemonico
        where ctv.Escenario = 'REAL'
        and FechaValoracion = {lafecha}
    
        """
    )

    col_schema = {
        'Cantidad': pl.Float64,
        'Portafolio': pl.Int64,
        'Name': pl.String,
        'ISIN': pl.String,
        'CodigoSBS': pl.String,
        'TipoRenta': pl.String,
        'ValorUnitario': pl.Float64,
        'ValorCausadoOriginal': pl.Float64,
        'ValorCausadoLocal': pl.Float64,
        'VPNOriginal': pl.Float64,
        'VPNLocal': pl.Float64,
        'Moneda': pl.String,
        'FechaVencimiento': pl.String

    }

    averpl = (
        pl.from_pandas(data=aver, schema_overrides=col_schema)
        .with_columns(pl.col('Moneda').replace({'DOL': 'USD', 'NSOL': 'PEN'}))
    )

    return averpl


# query = f"""
#          select
#                 Valor as Patrimonio,
#                 CodigoPortafolioSBS as Fondo,
#                 Fecha
#             from ValorIndicador
#             where Fecha={fch_pat}
#             and CodigoPortafolioSBS IN ('1','2','3')
#             and CodigoIndicador = 'VALFON'
#
# """
#
# [cols, aea] = query_sit(query)
#
# dfp_pat = pl.from_pandas(pd.DataFrame.from_records(aea, columns=cols))