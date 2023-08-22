import pandas as pd
import numpy as np
import datetime as dt
from dateutil.relativedelta import relativedelta
import os
import matplotlib.pyplot as plt
import mplcyberpunk

plt.style.use('cyberpunk')


class magicFormula():

    def __init__(self, dataFinal, balanceamento, numeroAtivos, filtroLiquidez, dataInicial = None, caminhoDados = None):

        try:

            dataInicial = dt.datetime.strptime(dataInicial, '%Y-%m-%d').date()
            dataFinal = dt.datetime.strptime(dataFinal, '%Y-%m-%d').date()

        except:

            dataFinal = dt.datetime.strptime(dataFinal, '%Y-%m-%d').date()

        self.dataFinal = dataFinal
        self.dataInicial = dataInicial
        self.filtroLiquidez = filtroLiquidez
        self.balanceamento = balanceamento
        self.numeroAtivos = numeroAtivos

        os.chdir(caminhoDados)


    def pegando_dados(self):
        
        dadosEmpresas = pd.read_parquet('dados_empresas.parquet')
        dadosEmpresas['data'] = pd.to_datetime(dadosEmpresas['data']).dt.date
        dadosEmpresas['ticker'] = dadosEmpresas['ticker'].astype(str)
        dadosEmpresas['ebit_ev'] = dadosEmpresas['ebit_ev'].astype(float)
        dadosEmpresas['roic'] = dadosEmpresas['roic'].astype(float)

        if self.dataInicial != None:

            dadosEmpresas = dadosEmpresas[(dadosEmpresas['data'] >= self.dataInicial) & (dadosEmpresas['data'] <= self.dataFinal)]

            totalAnos = self.dataFinal - self.dataInicial
            totalAnos = totalAnos.total_seconds()/31536000

        else:

            dadosEmpresas = dadosEmpresas[(dadosEmpresas['data'] >= (min(dadosEmpresas['data']) + relativedelta(month=+2))) & (dadosEmpresas['data'] <= self.dataFinal)]

            totalAnos = self.dataFinal - (min(dadosEmpresas['data']) + relativedelta(month=+2)).date()
            totalAnos = totalAnos.total_seconds()/31536000

        dadosIbov = pd.read_parquet('dados_ibov.parquet')
        dadosIbov['data'] = pd.to_datetime(dadosIbov['data']).dt.date

        datasDisponiveis = np.sort(dadosEmpresas['data'].unique())
        periodosDisponiveis = [datasDisponiveis[i] for i in range(0, len(datasDisponiveis), self.balanceamento)]

        dadosEmpresas = dadosEmpresas[dadosEmpresas['data'].isin(periodosDisponiveis)]
        dadosIbov = dadosIbov[dadosIbov['data'].isin(periodosDisponiveis)]
        
        self.periodosDisponiveis = periodosDisponiveis
        self.totalAnos = totalAnos
        self.dadosEmpresas = dadosEmpresas
        self.dadosIbov = dadosIbov

    def filtrando_liquidez(self):

        self.dadosEmpresas = self.dadosEmpresas[self.dadosEmpresas['volume_negociado'] > self.filtroLiquidez]

    def calculando_retornos(self):

        retornoEmpresas = self.dadosEmpresas
        retornoEmpresas['retorno'] = retornoEmpresas.groupby('ticker')['preco_fechamento_ajustado'].pct_change()
        retornoEmpresas['retorno'] = retornoEmpresas.groupby('ticker')['retorno'].shift(-1)

        self.dadosEmpresas = retornoEmpresas

    def criando_carteiras(self):

        dfDadosEmpresas = self.dadosEmpresas

        dfDadosEmpresas = dfDadosEmpresas.assign(TICKER_PREFIX = dfDadosEmpresas['ticker'].str[:4])
        dfDadosEmpresas = dfDadosEmpresas.loc[dfDadosEmpresas.groupby(['data', 'TICKER_PREFIX'])['volume_negociado'].idxmax()]
        dfDadosEmpresas = dfDadosEmpresas.drop('TICKER_PREFIX', axis=1)

        dfDadosEmpresas['ranking_ev_ebit'] = dfDadosEmpresas.groupby('data')['ebit_ev'].rank(ascending= False)
        dfDadosEmpresas['ranking_roic'] = dfDadosEmpresas.groupby('data')['roic'].rank(ascending= False)

        dfDadosEmpresas['ranking_final'] = dfDadosEmpresas['ranking_ev_ebit'] + dfDadosEmpresas['ranking_roic']
        dfDadosEmpresas['ranking_final'] = dfDadosEmpresas.groupby('data')['ranking_final'].rank(ascending= True)
        
        self.dadosEmpresas = dfDadosEmpresas
        self.carteiras = dfDadosEmpresas[dfDadosEmpresas['ranking_final'] <= self.numeroAtivos]

    def calculando_rentabilidade(self):
        
        rentabilidadeCarteira = self.carteiras.groupby('data')['retorno'].mean()
        rentabilidadeCarteira = rentabilidadeCarteira.to_frame()

        rentabilidadeCarteira['modelo'] = (rentabilidadeCarteira['retorno'] + 1).cumprod() - 1
        rentabilidadeCarteira = rentabilidadeCarteira.shift(1)
        rentabilidadeCarteira = rentabilidadeCarteira.dropna()

        retornoIbov = self.dadosIbov['fechamento'].pct_change().dropna()
        rentabilidadeIbov = (retornoIbov + 1).cumprod() - 1

        rentabilidadeCarteira['ibovespa'] = rentabilidadeIbov.values
        rentabilidadeCarteira = rentabilidadeCarteira.drop('retorno', axis= 1)

        self.rentabilidadeCarteira = rentabilidadeCarteira
        self.rentabilidadeAno = (1 + rentabilidadeCarteira['modelo'].iloc[-1]) ** (1/self.totalAnos) - 1 #pensar uma forma de calcular ano a ano


if __name__ == '__main__':

    backtest = magicFormula(dataFinal= '2023-06-30', dataInicial= '2015-12-24', balanceamento= 21, numeroAtivos= 10, filtroLiquidez= 1000000, caminhoDados= r'C:\Users\Caio\Documents\dev\github\backtest_magic_formula')

    backtest.pegando_dados()
    backtest.filtrando_liquidez()
    backtest.calculando_retornos()
    backtest.criando_carteiras()
    backtest.calculando_rentabilidade()

    carteira = backtest.carteiras
    dataCarteira = backtest.periodosDisponiveis[-1] #Datas que as carteiras foram geradas.

    print('-------------------------------------------------------------------------------------------------------------------------------------------------')
    print(f'NUMERO DE CARTEIRAS GERADAS: {len(backtest.periodosDisponiveis)}')
    print('-------------------------------------------------------------------------------------------------------------------------------------------------')
    print(f'CARTEIRA RECOMENDADA PELO MODELO NA DATA: {dataCarteira}')
    print('-------------------------------------------------------------------------------------------------------------------------------------------------')
    print(carteira[carteira['data'] == dataCarteira])
    print('-------------------------------------------------------------------------------------------------------------------------------------------------')
    print(f'RENTABILIDADE POR ANO DA CARTEIRA: {backtest.rentabilidadeAno * 100:.2f}%')
    print('-------------------------------------------------------------------------------------------------------------------------------------------------')

    backtest.rentabilidadeCarteira.plot()
    plt.show()