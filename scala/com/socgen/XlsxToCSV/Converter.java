package com.socgen.XlsxToCSV;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;

public class Converter {
    /**
     *
     * Convert XLSX TO CSV
     * @param args
     * @throws Exception
     */
    public static void main(String [] args ) throws Exception{
        FileOutputStream out = new FileOutputStream(args[1]);
        int sheetIdx = 1 ;
        File xlsxFile = new File(args[0]);
        FileInputStream fileInStream = new FileInputStream(xlsxFile);
        XSSFWorkbook workBook = new XSSFWorkbook(fileInStream);
        XSSFSheet selSheet = workBook.getSheetAt(sheetIdx);
        Iterator<Row> rowIterator = selSheet.iterator();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Iterator<Cell> cellIterator = row.cellIterator();
            StringBuffer sb = new StringBuffer();
            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                if (sb.length() != 0) {
                    sb.append(",");
                }
                switch (cell.getCellTypeEnum()) {
                    case STRING:
                        sb.append(cell.getStringCellValue());
                        break;
                    case NUMERIC:
                        sb.append(cell.getNumericCellValue());
                        break;
                    case BOOLEAN:
                        sb.append(cell.getBooleanCellValue());
                        break;
                    default:
                }
            }
            if(sb.toString().contains("Données à compléter par le Métier")||sb.toString().contains("Commentaire,données à anonymiser")
                    ||sb.toString().isEmpty()
                    )
            {
                //None
            }
            else {
                out.write(sb.toString().getBytes() );
                out.write("\n".getBytes());
            }
        }
        workBook.close();
    }
}
