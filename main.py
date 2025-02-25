import pandas as pd
import logging
from dotenv import load_dotenv
import sys
from config.db import DBConnection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
load_dotenv()

def main():
    try:
        if len(sys.argv) == 3:
            archive = sys.argv[2]
            logger.info(f'Starting process for document {archive}')
        else:
            logger.error('Wrong number of arguments')
            return
        db = DBConnection(sys.argv[1])
        motor = db.create_motor()
        # insertando registros de ventas
        dfSales = pd.read_excel(archive, sheet_name='Ventas')
        dfSales.insert(1, 'local', 'ñoño')
        logger.info(f"Reading {dfSales.shape[0]} records for ventas")
        dfSales.to_sql('ventas', con=motor, if_exists='append', index=False)
        logger.info('Data saved for table ventas')
        # insertando registros de pagos
        dfPayments = pd.read_excel(archive, sheet_name='Pagos')
        dfPayments.insert(1, 'local', 'ñoño')
        logger.info(f"Reading {dfPayments.shape[0]} records for pagos")
        dfPayments.to_sql('pagos', con=motor, if_exists='append', index=False)
        logger.info('Data saved for table pagos')
        # insertando registros de por pagar
        dfUnpaid = pd.read_excel(archive, sheet_name='Por Pagar')
        dfUnpaid.insert(1, 'local', 'ñoño')
        logger.info(f"Reading {dfUnpaid.shape[0]} records for por_pagar")
        dfUnpaid.to_sql('por_pagar', con=motor, if_exists='append', index=False)
        logger.info('Data saved for table por_pagar')
    except Exception as e:
        logger.error(e)
    finally:
        logger.info('End process')

if __name__ == '__main__':
    main()