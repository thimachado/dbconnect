<#--------------------------------------------------------------------*
| Script de backup                                                    |
|                                        |
| Feito por:Thiago F Machado                                       |
| Data: 13/10/2017                                                    |
*--------------------------------------------------------------------#>


#region parametros backup
param( 
[bool]$executa=$true,
[string] $revendaManual,
[string] $tipoManual
)

#Parâmetros database
$global:serverdb = ""
$global:db = ""
$global:usr = ""
$global:pw =""

#Outros parametros
$global:server= hostname #server name
$global:weekday = (Get-Date).DayOfWeek.value__
##DEFINICOES
$titulo = "BACKUP_PROMAXCLOUD"
$global:msgPromax = ""
$logTime = Get-Date -Format "dd-MM-yyyy_HH-mm-ss"
if($revendaManual -ne "" -and $tipoManual -ne ""){
    $logFile = "W:\Backup\log\"+$titulo+"_"+$revendaManual+"_"+$logTime+".txt"
}else{
    $logFile = "W:\Backup\log\"+$titulo+"_"+$logTime+".txt"
}
$dirBin = "W:\Backup\bin\"
$dirDestinoBase = "W:\Backup\TEMP\"
$dirArquivosCSV = "W:\BACKUP\scripts\"
$tempoLoop = 5 #segundos
$tempoWaiting = 60 #60 segundos
$tempoEspera = 180 #180 segundos
$tempoEsperaPromax1 = 240 #240 segundos
$tempoEsperaPromax2 = 90 #90 segundos
$infoDiretorios = $dirArquivosCSV+"diretorios.csv"
$infoServices = $dirArquivosCSV+"servicos.csv"
$diretorios = Import-Csv $infoDiretorios
$servicos = Import-Csv $infoServices
#endregion

#region connect sql
Function ConnectSQL(){
    try{

        $conn_string = "Server=$serverdb;Database=$db;User Id=$usr;Password=$pw;"
        $conn = New-Object System.Data.SqlClient.SqlConnection
        $conn.ConnectionString = $conn_string
        $conn.Open() 
        $cmd = New-Object System.Data.SqlClient.SqlCommand
        $cmd.Connection = $conn

        $a = $cmd.Connection.State

        $conn.Close()
    
        Return $a
    }catch{
        Return "Closed"
    }
}
#endregion

#region querysql
Function QuerySQL(){
$conn_string = "Server=$serverdb;Database=$db;User Id=$usr;Password=$pw;"
$conn = New-Object System.Data.SqlClient.SqlConnection
$conn.ConnectionString = $conn_string
$conn.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.Connection = $conn

$unidades = @()

#query
$querysql = @"
use backupmanager
SELECT *
FROM backupmanagers AS backupmanager
JOIN units AS unit ON backupmanager.unit_id = unit.id
JOIN [dbo].servers AS srv ON unit.server_id = srv.id
JOIN [dbo].types AS tp ON backupmanager.type_id = tp.id
where server_name= '$server' AND  day_id = '$weekday'AND backup_status = '1'
ORDER BY backup_hour;  
"@
$cmd.CommandText = $querysql
$reader = $cmd.ExecuteReader()

while ($reader.Read()){
$props = @{'id'= $reader['id']
'unit_name' = $reader['unit_name']
'unit_disk' = $reader['unit_disk']
'unit_destiny' = $reader['unit_destiny']
'unit_tolerance' = $reader['unit_tolerance']
'unit_eventid' = $reader['unit_eventid']
'backup_hour' = $reader['backup_hour']
'backup_status' = $reader['backup_status']
'day_id' = $reader['day_id']
'type_id' = $reader['type_id']
'type_name' = $reader['type_name']
'server_id' = $reader['server_id']
'unit_id' = $reader['unit_id']
'status' = 'new'
}
$unidades+= New-Object -TypeName PSObject -Property $props
$revenda = $props
}
$conn.Close()

return $unidades
}
#endregion

#region cache json

function CacheJson(){
[int]$criterio = get-date -format HH

$conn_string = "Server=$serverdb;Database=$db;User Id=$usr;Password=$pw;"
$conn = New-Object System.Data.SqlClient.SqlConnection
$conn.ConnectionString = $conn_string
$conn.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.Connection = $conn
$revendacache = @()
if ($weekday -eq 6){
$nextday = 0
}else{
$nextday = $weekday + 1
}

$cache = @"
use backupmanager
SELECT *
FROM backupmanagers AS backupmanager
JOIN units AS unit ON backupmanager.unit_id = unit.id
JOIN [dbo].servers AS srv ON unit.server_id = srv.id
JOIN [dbo].types AS tp ON backupmanager.type_id = tp.id
where server_name= '$server' AND  day_id = '$nextday'AND backup_status = '1'
ORDER BY backup_hour;  
"@

$cmd.CommandText = $cache
$reader = $cmd.ExecuteReader()

while ($reader.Read()){
$props = @{'id'= $reader['id']
'unit_name' = $reader['unit_name']
'unit_disk' = $reader['unit_disk']
'unit_destiny' = $reader['unit_destiny']
'unit_tolerance' = $reader['unit_tolerance']
'unit_eventid' = $reader['unit_eventid']
'backup_hour' = $reader['backup_hour']
'backup_status' = $reader['backup_status']
'day_id' = $reader['day_id']
'type_id' = $reader['type_id']
'type_name' = $reader['type_name']
'server_id' = $reader['server_id']
'unit_id' = $reader['unit_id']
'status' = 'new'
}
$revendacache += New-Object -TypeName PSObject -Property $props
$revenda = $props
}


if($criterio -ge 11 -and $criterio  -lt 23){ 
#json
$revendacache| ConvertTo-Json | Out-File "W:\BACKUP\scripts\revendas.json"
}
if($weekday -eq "6"){
$revendacache| ConvertTo-Json | Out-File "W:\BACKUP\scripts\cachedomingo.json"
}
}
#endregion

#region logzabbix

function LogZabbix{

param( 
       $revenda,
       $logzabbix
      )

$conn_string = "Server=$serverdb;Database=$db;User Id=$usr;Password=$pw;"
$conn = New-Object System.Data.SqlClient.SqlConnection
$conn.ConnectionString = $conn_string
$conn.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.Connection = $conn
$hour = $(([datetime]$revenda.backup_hour).ToString("HH:mm"))
$querysql = @"
use backupmanager
UPDATE backupstatus
SET backupstatus_situation = ' $logzabbix', updated_at = current_timestamp, backupstatus_hour = '$hour', type_name = '$($revenda.type_name)'
WHERE unit_id = '$($revenda.unit_id)';
"@
$cmd.CommandText = $querysql
$reader = $cmd.ExecuteReader()
$conn.Close()

}

#endregion


#region logalltime

function LogAllTime{

param( 
       $revenda,
       $logzabbix
      )

$conn_string = "Server=$serverdb;Database=$db;User Id=$usr;Password=$pw;"
$conn = New-Object System.Data.SqlClient.SqlConnection
$conn.ConnectionString = $conn_string
$conn.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.Connection = $conn
$hour = $(([datetime]$revenda.backup_hour).ToString("HH:mm"))
$day = $(([datetime]$revenda.backup_hour).ToString("dd-MM-yyyy"))
$querysql = @"
use backupmanager
UPDATE alltime_backupstatus
SET backupstatus_situation = '  $logzabbix', backupstatus_hour = '$hour', type_name = '$($revenda.type_name)', backupstatus_date = '$day'
WHERE unit_id = '$($revenda.unit_id)' and backupstatus_hour = '$($hour)' and backupstatus_date = '$day';
"@
$cmd.CommandText = $querysql
$reader = $cmd.ExecuteReader()
$conn.Close()

}


#endregion

#region insertlogalltime

function InsertLogAllTime{
param( 
       $revenda,
       $logzabbix
      )

$conn_string = "Server=$serverdb;Database=$db;User Id=$usr;Password=$pw;"
$conn = New-Object System.Data.SqlClient.SqlConnection
$conn.ConnectionString = $conn_string
$conn.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.Connection = $conn
$hour = $(([datetime]$revenda.backup_hour).ToString("HH:mm"))
$day = $(([datetime]$revenda.backup_hour).ToString("dd-MM-yyyy"))
$currenttime = Get-Date -Format "dd-MM-yyyy HH:mm:ss"
$querysql = @"
use backupmanager
INSERT INTO alltime_backupstatus (unit_id, server_id, backupstatus_situation, backupstatus_hour, type_name, backupstatus_date) VALUES
('$($revenda.unit_id)','$($revenda.server_id)' ,'$logzabbix', '$hour','$($revenda.type_name)','$day');
"@
$cmd.CommandText = $querysql
$reader = $cmd.ExecuteReader()
$conn.Close()

}

#endregion


#region register backup start copy
function RegisterBackupStartCopy{
param( 
       $revenda
      )
#Conexão com o banco
$conn_string = "Server=$serverdb;Database=$db;User Id=$usr;Password=$pw;"
$conn = New-Object System.Data.SqlClient.SqlConnection
$conn.ConnectionString = $conn_string
$conn.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.Connection = $conn

$querysql = @"
use backupmanager
UPDATE backupstatus
SET  backupstatus_startcopy = current_timestamp
WHERE unit_id = '$($revenda.unit_id)';
"@
$cmd.CommandText = $querysql
$reader = $cmd.ExecuteReader()
$conn.Close()


$conn.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.Connection = $conn
$hour = $(([datetime]$revenda.backup_hour).ToString("HH:mm"))
$day = $(([datetime]$revenda.backup_hour).ToString("dd-MM-yyyy"))
$dayz =((Get-Date -Format "yyyy-MM-dd HH:mm:ss:mm"))
$querysql = @"
use backupmanager
UPDATE alltime_backupstatus
SET  backupstatus_startcopy = '$dayz'
WHERE unit_id = '$($revenda.unit_id)' and backupstatus_hour = '$($hour)' and backupstatus_date = '$day';
"@
$cmd.CommandText = $querysql
$reader = $cmd.ExecuteReader()
$conn.Close()


}
#endregion

#region register backup end copy
function RegisterBackupEndCopy{
param( 
       $revenda
      )
#Conexão com o banco
$conn_string = "Server=$serverdb;Database=$db;User Id=$usr;Password=$pw;"
$conn = New-Object System.Data.SqlClient.SqlConnection
$conn.ConnectionString = $conn_string
$conn.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.Connection = $conn
$hour = $(([datetime]$revenda.backup_hour).ToString("HH:mm"))
$day = $(([datetime]$revenda.backup_hour).ToString("dd-MM-yyyy"))
$querysql = @"
use backupmanager
UPDATE backupstatus
SET  backupstatus_endcopy = current_timestamp
WHERE unit_id = '$($revenda.unit_id)';
"@
$cmd.CommandText = $querysql
$reader = $cmd.ExecuteReader()
$conn.Close()


$conn.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.Connection = $conn
$hour = $(([datetime]$revenda.backup_hour).ToString("HH:mm"))
$day = $(([datetime]$revenda.backup_hour).ToString("dd-MM-yyyy"))
$dayz =((Get-Date -Format "yyyy-MM-dd HH:mm:ss:mm"))
$querysql = @"
use backupmanager
UPDATE alltime_backupstatus
SET  backupstatus_endcopy = '$dayz'
WHERE unit_id = '$($revenda.unit_id)' and backupstatus_hour = '$($hour)' and backupstatus_date = '$day';
"@
$cmd.CommandText = $querysql
$reader = $cmd.ExecuteReader()
$conn.Close()


}
#endregion

#region register start backup
function RegisterStartBackup{
param( 
       $revenda
      )

$startedat = Get-Date -Format "dd/MM/yyyy HH:mm:ss"

#Conexão com o banco
$conn_string = "Server=$serverdb;Database=$db;User Id=$usr;Password=$pw;"
$conn = New-Object System.Data.SqlClient.SqlConnection
$conn.ConnectionString = $conn_string
$conn.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.Connection = $conn

$querysql = @"
use backupmanager
UPDATE backupstatus
SET  started_at = '$startedat'
WHERE unit_id = '$($revenda.unit_id)';
"@
$cmd.CommandText = $querysql
$reader = $cmd.ExecuteReader()
$conn.Close()

}
#endregion

#region logdb
function LogDB(){
param( 
       $revenda,
       $logdb
      )
$conn_string = "Server=$serverdb;Database=$db;User Id=$usr;Password=$pw;"
$conn = New-Object System.Data.SqlClient.SqlConnection
$conn.ConnectionString = $conn_string
$conn.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.Connection = $conn
$hour = $(([datetime]$revenda.backup_hour).ToString("HH:mm"))
$day = $(([datetime]$revenda.backup_hour).ToString("dd-MM-yyyy"))
$querysql = @"
use backupmanager
  INSERT INTO backuplogs(unit_id, type_id, server_id,backuplog_description,backuplog_hour, created_at, backuplog_day) VALUES
( '$($revenda.unit_id)','$($revenda.type_id)','$($revenda.server_id)','$logdb', '$hour', CURRENT_TIMESTAMP, '$day')
"@

$cmd.CommandText = $querysql
$reader = $cmd.ExecuteReader()

$conn.Close()

}
#endregion

#region add-content
Function Add-contentRevenda{
    param(
        [string]$file,
        [string]$value
    )
    if($revendaManual -ne "" -and $tipoManual -ne ""){
        Add-Content ("W:\Backup\log\"+$titulo+"_"+$revendaManual+"_"+$logTime+".txt") -Value ($value)
    }else{
        Add-Content $file -Value ($value)
        Add-Content ("W:\Backup\log\"+$titulo+"_"+$revenda.unit_name+"_"+$logTime+".txt") -Value ($value)
    }

}
#endregion

#region inicia logs
Function IniciaLog {
    if($revendaManual -ne "" -and $tipoManual -ne ""){
        Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss") + " INICIO BACKUP MANUAL")
    }else{
        Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss") + " INICIO BACKUP AUTOMATICO")
        }
    if($revendaManual -ne "" -and $tipoManual -ne ""){
      
    }else{
  
        Add-content $logFile -value ("Parametros Execução: " + $executa)
        Add-content $logFile -value ("Backup Automático - Agendado")
        Add-content $logFile -value ("Tempo Entre Loop: " +$tempoLoop + " segundos")
        Add-content $logFile -value ("Tempo Waiting: " +$tempoWaiting + " segundos")
        Add-content $logFile -value ("Tempo Espera Promax - Etapa 01: " +$tempoEsperaPromax1 + " segundos")
        Add-content $logFile -value ("Tempo Espera Promax - Etapa 02: " +$tempoEsperaPromax2 + " segundos")
        Add-content $logFile -value ("Tempo Maximo Espera: " +$tempoEspera + " minutos")

        $revendas = QuerySQL

        Foreach ($revenda in $revendas) {
        Add-content $logFile -value ("RETORNO DO SELECT NO MÉTODO QUERYSQL")
        Add-content $logFile -value ($revenda.unit_name + ", " + $revenda.unit_disk + ", " + $revenda.type_name + ", " + $revenda.day_id + ", " + $revenda.backup_hour)
        }

                
        #Add-content $logFile -value ("")
    }
}
#endregion

#region adiciona info
Function AdicionaInfo{
$revendas|Add-Member -NotePropertyName Inicio -NotePropertyValue " "
$revendas|Add-Member -NotePropertyName Fim -NotePropertyValue " "
$revendas|Add-Member -NotePropertyName Diretorio -NotePropertyValue " "
$revendas|Add-Member -NotePropertyName Sequencia -NotePropertyValue " "

}
#endregion

#region imprime resumo
Function ImprimeResumo(){

    Add-content $logFile -value ("")
    Add-content $logFile -value ("--- RESUMO ---")
    Add-content $logFile -value ("Revenda,Tipo,Inicio,Fim,Sequencia")
    Foreach ($revenda in $revendas) {
        if($revenda.status -eq "ok"){ 
            Add-content $logFile -value ($revenda.unit_name+","+$revenda.type_name +"," + $revenda.Inicio + "," + $revenda.Fim  + "," + $revenda.Sequencia ) 
        }
    }
    Add-content $logFile -value ("")
    Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Fim da execucao")
}

#endregion

#region atualiza horarios
Function AtualizaHorarios(){
    $revendasUpdate = $revendas
    $index = 0;
    Foreach ($revenda in $revendas) {
        $revenda.backup_hour = $revendasUpdate[$index].backup_hour
        $index++
    }
}
#endregion

#region verifica diretório vazio
Function VerificaDiretorioVazio(){
    param(
      $dirDestino 
        )

[int]$hour = get-date -format HH

if($hour -lt 12 -or $hour -gt 14){
 return $true

  }Else{
  if($hour -gt 12 -or $hour -lt 14){
    $tam = (Get-ChildItem $dirDestino  -Recurse |Measure-Object -Property Length -sum).Sum
    if([string]::IsNullOrEmpty($tam)) {$tam=0}
    if($tam -eq 0){
        return $true
    }else{
        return $false
    }
    }
    }
}
#endregion

#region verifica promax liberado
Function VerificaPromaxLiberado(){
    param (
        $revenda
        )

    if($executa){
        $pinfo = New-Object System.Diagnostics.ProcessStartInfo
        $pinfo.FileName = "$dirBin\checkPromaxBackup.exe"
        $pinfo.RedirectStandardError = $true
        $pinfo.RedirectStandardOutput = $true
        $pinfo.UseShellExecute = $false
        $pinfo.Arguments = $revenda.unit_name+".promaxcloud.com.br"
        $p = New-Object System.Diagnostics.Process
        $p.StartInfo = $pinfo
        $p.Start() | Out-Null
        $p.WaitForExit()
        $stdout = $p.StandardOutput.ReadToEnd()
        $stderr = $p.StandardError.ReadToEnd() 
        $saida=$p.ExitCode
        if($saida -eq "0"){
            $global:msgPromax = $stdout+$stderr
            return $true
        }elseif($saida -eq "1"){
            $global:msgPromax = $stdout+$stderr
            return $false
        }elseif($saida -eq "2"){
            $global:msgPromax = $stdout+$stderr
            return $false
        }elseif($saida -eq "3"){
            $global:msgPromax = $stdout+$stderr
            return $true
        }
    }else{
        Write-Host ($dirBin+"checkPromaxBackup.exe " + $revenda.unit_name+".promaxcloud.com.br")
        return $true
    }
}

#endregion

#region check-path
Function CheckPath(){
    param(
        [string]$path,
        [bool] $criar
    )
	If (Test-Path -path $path){        
			
        return $true
    }else{
            #Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Diretório não existe: " +$path)
        if($criar){
            if($executa){
                New-Item -ItemType Directory -Path $path -Force -ErrorAction SilentlyContinue | Out-Null
			    if(-not $?) {
					    Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro criando diretório " + $path )
					    Add-content $logFile -value ($error[0].exception.message)
				    }
				    else {
					    Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Criando diretório: "+$path)
				    } 			
			    }else{
                Write-Host New-Item -ItemType Directory -Path $path -Force
            }
        }else{
            return $false
        }
    }	
}
#endregion

#region parar serviços
Function PararServicos(){
    param( 
        $revenda
        )

    $timeoutServico = 0
    foreach($servico in $servicos){
        if($servico.tipo -eq "servico"){
            $service = (($servico.servico).replace("`$revenda",$revenda.unit_name))
            if (Get-Service $service -ErrorAction SilentlyContinue){
                if ($executa){                
                    Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Parando Serviço: "+$service)
                    $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Parando Serviço: "+$service)
                    LogDB $revenda $logdb
                    Stop-Service $service -Force -NoWait -ErrorAction SilentlyContinue | Out-Null
                    if(-not $?) {
					    Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Parando Serviço " + $service )
                       $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Parando Serviço " + $service )
                        LogDB $revenda $logdb
					    Add-contentRevenda $logFile -value ($error[0].exception.message)
                        $logdb = ($error[0].exception.message)
                        LogDB $revenda $logdb

				    }
                }else{
                    write-host Stop-Service $service -Force -NoWait
                }
            }else{
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Serviço "+$service + " não existe")
                   $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Serviço "+$service + " não existe")
                   LogDB $revenda $logdb
            }
            if([int]$timeoutServico -le [int]$servico.timeout){
                $timeoutServico = $servico.timeout
            }
        }elseif($servico.tipo -eq "IIS"){
            $nomeSiteIIS = (($servico.servico).replace("`$revenda",$revenda.unit_name))
            if ($executa){
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Parando Site IIS: "+$nomeSiteIIS)
                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Parando Site IIS: "+$nomeSiteIIS)
                LogDB $revenda $logdb
                Stop-Website $nomeSiteIIS -ErrorAction SilentlyContinue | Out-Null
                if(-not $?) {
					Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Parando Site IIS " + $nomeSiteIIS )
                    $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Parando Site IIS " + $nomeSiteIIS )
                    LogDB $revenda $logdb

					Add-contentRevenda $logFile -value ($error[0].exception.message)
                    $logdb = ($error[0].exception.message)
                    LogDB $revenda $logdb
				}
            }else{
                write-host Stop-Website $nomeSiteIIS
            }
            if([int]$timeoutServico -le [int]$servico.timeout){
                $timeoutServico = $servico.timeout
            }
        }
    }
    if ($executa){
        Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Aguardando "+$timeoutServico +" segundos...")
        $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Aguardando "+$timeoutServico +" segundos...")
        LogDB $revenda $logdb
        Start-Sleep $timeoutServico -ErrorAction SilentlyContinue | Out-Null
            if(-not $?) {
				Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Aguardando " + $timeoutServico + " segundos..." )
                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Aguardando " + $timeoutServico + " segundos..." )
                LogDB $revenda $logdb
				Add-contentRevenda $logFile -value ($error[0].exception.message)
                $logdb = ($error[0].exception.message)
                LogDB $revenda $logdb
			}
    }
    else{
        Write-host Start-Sleep $timeoutServico
    }

    
    foreach($servico in $servicos){
        
        if($servico.tipo -eq "renArquivo"){
                $origem= $revenda.unit_disk+ ":\" +$revenda.unit_name+ "\" + $servico.servico
                $destino= $revenda.unit_disk + ":\" +$revenda.unit_name+"\"+$servico.servico+".temp"
            if ($executa){
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Renomeando Arquivo: "+$origem + " para "+ $destino)
                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Renomeando Arquivo: "+$origem + " para "+ $destino)
                LogDB $revenda $logdb
                Rename-Item  $origem $destino -ErrorAction SilentlyContinue | Out-Null
                if(-not $?) {
					Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Renomeando Arquivo: "+$origem + " para "+ $destino)
                    $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Renomeando Arquivo: "+$origem + " para "+ $destino)
                    LogDB $revenda $logdb
					Add-contentRevenda $logFile -value ($error[0].exception.message)
                    $logdb = ($error[0].exception.message)
                    LogDB $revenda $logdb
				}
            }
            else{
                $cmdRen = "Rename-Item " + $origem + " " + $destino
                Write-Host $cmdRen 
            }
        }elseif($servico.tipo -eq "delArquivo"){
            $origem=$revenda.unit_disk+":\"+$revenda.unit_name+"\"+$servico.servico
            if ($executa){
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Removendo Arquivos. Máscara: "+$origem)
                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Removendo Arquivos. Máscara: "+$origem)
                LogDB $revenda $logdb
                Remove-Item $origem -ErrorAction SilentlyContinue | Out-Null
                if(-not $?) {
					Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Removendo Arquivos. Máscara: "+$origem)
                    $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Removendo Arquivos. Máscara: "+$origem)
                    LogDB $revenda $logdb
					Add-contentRevenda $logFile -value ($error[0].exception.message)
                    $logdb = ($error[0].exception.message)
                    LogDB $revenda $logdb				
}
            }else{
                $cmdDel = "Remove-Item "+ $origem
                Write-Host $cmdDel 
            }
        }elseif($servico.tipo -eq "criaArquivo"){
            $origem=$revenda.unit_disk+":\"+$revenda.unit_name+"\"+$servico.servico
            if ($executa){
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Criando Arquivo: "+$origem)
                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Criando Arquivo: "+$origem)
                 LogDB $revenda $logdb	
                New-Item $origem -ErrorAction SilentlyContinue | Out-Null
                if(-not $?) {
					Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Criando Arquivo: "+$origem)
                     $logdb =  ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Criando Arquivo: "+$origem)
                     LogDB $revenda $logdb	
					Add-contentRevenda $logFile -value ($error[0].exception.message)
                    $logdb = ($error[0].exception.message)
                    LogDB $revenda $logdb	
				}
            }else{
                $cmdCreate = "New-Item "+ $origem
                Write-Host $cmdCreate 
            }
        }elseif($servico.tipo -eq "aguarda"){
           if ($executa){
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Aguardando "+$servico.servico +" segundos...")
                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Aguardando "+$servico.servico +" segundos...")
                LogDB $revenda $logdb	
                Start-Sleep $servico.servico -ErrorAction SilentlyContinue | Out-Null
                if(-not $?) {
					Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Aguardando "+$servico.servico +" segundos...")
                    $logdb =((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Aguardando "+$servico.servico +" segundos...")
                    LogDB $revenda $logdb	
					Add-contentRevenda $logFile -value ($error[0].exception.message)
                    $logdb = ($error[0].exception.message)
                    LogDB $revenda $logdb	
				}
            }else{
                Write-host Start-Sleep $servico.servico
            }
            
        }elseif($servico.tipo -eq "killProcesso"){
            if ($executa){
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Encerrando processos. Máscara: "+$revenda.unit_disk+":\"+$revenda.unit_name+"\"+$servico.servico)
                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Encerrando processos. Máscara: "+$revenda.unit_disk+":\"+$revenda.unit_name+"\"+$servico.servico)
                LogDB $revenda $logdb	

                Get-Process | Where-Object {$_.Path -like $revenda.unit_disk+":\"+$revenda.unit_name+"\"+$servico.servico } | Stop-Process -Force -ErrorAction SilentlyContinue | Out-Null
                if(-not $?) {
					Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Encerrando processos. Máscara: "+$revenda.unit_disk+":\"+$revenda.unit_name+"\"+$servico.servico)
                   $logdb =  ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Encerrando processos. Máscara: "+$revenda.unit_disk+":\"+$revenda.unit_name+"\"+$servico.servico)
                   LogDB $revenda $logdb	
				   Add-contentRevenda $logFile -value ($error[0].exception.message)
                   $logdb = ($error[0].exception.message)			    
                   LogDB $revenda $logdb	

}
            }else{
                $cmdKill = "Get-Process | Where-Object {$_.Path -like "+$revenda.unit_disk+":\"+$revenda.unit_name+"\"+$servico.servico+" } | Stop-Process -Force "
                Write-Host $cmdKill
            }

         }
    }
}
#endregion

#region incrementa sequência backup
Function IncrementaSeqBackup(){
    param( 
        $revenda
        )
   $caminho = $dirArquivosCSV+"index\"+$revenda.unit_name
    if(Test-Path $caminho){
        $atual = [int] (Get-Content $caminho )
    }else{
        $atual = 0
    }
    $stream = [System.IO.StreamWriter] $caminho
    $stream.Write($atual+1)
    $stream.Close()
    $revenda.Sequencia = ($atual+1)
        
    
}
#endregion

#region zera sequência backup
Function ZeraSeqBackup(){
    param( 
        $revenda
        )
    $caminho = $dirArquivosCSV+"index\"+$revenda.unit_name
    $stream = [System.IO.StreamWriter] $caminho
    $stream.Write("0")
    $stream.Close()

}

#endregion

#region ler sequência backup
Function LerSeqBackup(){
    param( 
        $revenda
        )
    try{
        $caminho = $dirArquivosCSV+"index\"+$revenda.unit_name
        if(Test-Path $caminho){
            return [int] (Get-Content $caminho )
        }else{
            return 0
        }
    }
    catch{
        return 0
    }
    
}
#endregion

#region cria evento de compactação
Function CriarEventoCompactacao{
    param(
        $revenda, $seqBackup
        )

      $cmdEventcreate = " /ID "+$revenda.unit_eventid+" /T INFORMATION /L APPLICATION /SO PromaxCloud /D `"Cópia backup concluída`r`nRevenda:"+$revenda.unit_name+"`r`nTipo:"+$revenda.type_name +"`r`nSequencia:" + $seqBackup + " `r`nCarimbo:"+(Get-Date -Format "dd-MM-yyyy_HH-mm-ss")   + " `r`nCaminho:W:\BACKUP\temp`""
    if ($executa){
        #Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Executando Comando: EVENTCREATE " + $cmdEventcreate )
        $pinfo = New-Object System.Diagnostics.ProcessStartInfo
        $pinfo.FileName = "EVENTCREATE.exe"
        $pinfo.RedirectStandardError = $true
        $pinfo.RedirectStandardOutput = $true
        $pinfo.UseShellExecute = $false
        $pinfo.Arguments = $cmdEventcreate.split(' ')
        $p = New-Object System.Diagnostics.Process
        $p.StartInfo = $pinfo
        $p.Start() | Out-Null
        $p.WaitForExit()
        $stdout = $p.StandardOutput.ReadToEnd()
        $stderr = $p.StandardError.ReadToEnd() 
        
        if(($stderr.Trim() -ne "") -and ($stderr.Trim() -ne $null)){
            Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro ao executar comando EVENTCREATE" + $cmdEventcreate )
			Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro ao executar comando EVENTCREATE" )
            $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro ao executar comando EVENTCREATE" )
            LogDB $revenda $logdb	
            Add-contentRevenda $logFile -value ($stderr)
            $logdb = ($stderr)
            LogDB $revenda $logdb	
        }else{
            Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Evento de compactação gerado com sucesso" )
            $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Evento de compactação gerado com sucesso" )
            LogDB $revenda $logdb	

             
       
[int]$hour = get-date -format HH
 if($hour -gt 11 -and $hour -lt 14){ 
 $bkpconcluido = Get-Date -Format "dd-MM-yyyy"
 New-Item ($revenda.unit_disk+":\"+$revenda.unit_name+"\Promax\tmp\work\promax_backup.concluido." +  $bkpconcluido ) -type file
 Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Arquivo promax_backup.concluido criado com sucesso." )
 $logdb =  ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Arquivo promax_backup.concluido criado com sucesso." )
 LogDB $revenda $logdb	

}

        }
    }else{
       Write-Host EVENTCREATE $cmdEventcreate }

}
#endregion

#region inicia serviços
Function IniciarServicos(){
   param( 
        $revenda
        )
    $timeoutServico = 0
    foreach($servico in $servicos){
        if($servico.tipo -eq "servico"){
            $service = (($servico.servico).replace("`$revenda",$revenda.unit_name))
            if (Get-Service $service -ErrorAction SilentlyContinue){
                if ($executa){
                    Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Iniciando Serviço: "+$service)
                    $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Iniciando Serviço: "+$service)
                     LogDB $revenda $logdb	
                    Start-Service $service -ErrorAction SilentlyContinue | Out-Null
                    if(-not $?) {
					    Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Iniciando Serviço: "+$service)
                        $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Iniciando Serviço: "+$service)
                        LogDB $revenda $logdb	
					    Add-contentRevenda $logFile -value ($error[0].exception.message)
                        $logdb =  ($error[0].exception.message)
                        LogDB $revenda $logdb	
				    }
                }else{
                    write-host Start-Service $service
                 }
            }else{
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Serviço "+$service + " não existe")
                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Serviço "+ $service + " não existe")
                LogDB $revenda $logdb	
            }
            if($timeoutServico -le $servico.timeout){
                $timeoutServico = $servico.timeout
            }
        }elseif($servico.tipo -eq "IIS"){
            $nomeSiteIIS = (($servico.servico).replace("`$revenda",$revenda.unit_name))
            if ($executa){
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Iniciando Site IIS: "+$nomeSiteIIS)
                $logdb =((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Iniciando Site IIS: "+$nomeSiteIIS)
                 LogDB $revenda $logdb	
                Start-Website $nomeSiteIIS -ErrorAction SilentlyContinue | Out-Null
                if(-not $?) {
					Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Iniciando Site IIS " + $nomeSiteIIS )
                    $logdb =((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Iniciando Site IIS " + $nomeSiteIIS )
                    LogDB $revenda $logdb	
					Add-contentRevenda $logFile -value ($error[0].exception.message)
                    $logdb = ($error[0].exception.message)
                    LogDB $revenda $logdb	
}
            }else{
                write-host Start-Website $nomeSiteIIS
            }
            if($timeoutServico -le $servico.timeout){
                $timeoutServico = $servico.timeout
            }
        }
    }
    
    foreach($servico in $servicos){
        
        if($servico.tipo -eq "renArquivo"){
                $origem= $revenda.unit_disk+ ":\" +$revenda.unit_name+ "\" + $servico.servico+".temp"
                $destino= $revenda.unit_disk + ":\" +$revenda.unit_name+"\"+$servico.servico
            if ($executa){
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Renomeando arquivo: "+$origem + " para "+ $destino)
                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Renomeando arquivo: "+$origem + " para "+ $destino)
                LogDB $revenda $logdb	
                Rename-Item  $origem $destino -ErrorAction SilentlyContinue | Out-Null
                if(-not $?) {
					Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Renomeando arquivo: "+$origem + " para "+ $destino)
                    $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Renomeando arquivo: "+$origem + " para "+ $destino)
                    LogDB $revenda $logdb	
					Add-contentRevenda $logFile -value ($error[0].exception.message)
                    $logdb =($error[0].exception.message)
                    LogDB $revenda $logdb	
				}
            }
            else{
                $cmdRen = "Rename-Item " + $origem + " " + $destino
                Write-Host $cmdRen 
            }
        }
    }
}
#endregion

#region cópia do backup
Function BackupRevenda(){
    param(
        $revenda
        )
$revenda.Inicio = Get-Date -Format "dd/MM/yyyy HH:mm:ss"
RegisterStartBackup $revenda 

Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" INICIANDO BACKUP " + $revenda.unit_name + " - "+ $revenda.type_name)
$logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" INICIANDO BACKUP " + $revenda.unit_name + " - "+ $revenda.type_name)
LogDB $revenda $logdb

$dirDestino = $dirDestinoBase + $revenda.unit_name

if(!(VerificaDiretorioVazio $dirDestino)){
        if ($executa){
         Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss") + " Diretório de destino não está vazio:" + "$dirDestino" + " Tamanho: " + [math]::round(((Get-ChildItem $dirDestino -Recurse |Measure-Object -Property Length -sum).Sum/1024MB),2) + "GB" ) 
         $logFile = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss") + " Diretório de destino não está vazio:" + "$dirDestino" + " Tamanho: " + [math]::round(((Get-ChildItem $dirDestino -Recurse |Measure-Object -Property Length -sum).Sum/1024MB),2) + "GB" ) 
         LogDB $revenda $logdb
         $logzabbix = '11'
         LogZabbix $revenda $logzabbix  
         LogAllTime $revenda $logzabbix 
         $podeExecutarBackup=$false
        }else{
            Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss") + " Diretório de destino não está vazio:" + "$dirDestino" + " Tamanho: " + [math]::round(((Get-ChildItem $dirDestino -Recurse |Measure-Object -Property Length -sum).Sum/1024MB),2) + "GB" ) 
            $logdb =((Get-Date -Format "dd/MM/yyyy HH:mm:ss") + " Diretório de destino não está vazio:" + "$dirDestino" + " Tamanho: " + [math]::round(((Get-ChildItem $dirDestino -Recurse |Measure-Object -Property Length -sum).Sum/1024MB),2) + "GB" ) 
            LogDB $revenda $logdb
            $logzabbix = '11'
            LogZabbix $revenda $logzabbix
            LogAllTime $revenda $logzabbix 
           $podeExecutarBackup=$false
        }
         $logdb =((Get-Date -Format "dd/MM/yyyy HH:mm:ss") + " Diretório de destino não está vazio:" + "$dirDestino" + " Tamanho: " + [math]::round(((Get-ChildItem $dirDestino -Recurse |Measure-Object -Property Length -sum).Sum/1024MB),2) + "GB" ) 
         LogDB $revenda $logdb
         $revenda.status = "waiting"
         $logzabbix = '11'
         LogZabbix $revenda $logzabbix
         LogAllTime $revenda $logzabbix 
         return $false
 
    }else{
        Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Verificando se há processos criticos no Promax  " + $revenda.unit_name)
         $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Verificando se há processos criticos no Promax  " + $revenda.unit_name)
         LogDB $revenda $logdb


        if(VerificaPromaxLiberado $revenda){
            if ($executa){
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Backup liberado para prosseguir. Aguardando " + $tempoEsperaPromax1+" segundos...")
                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Backup liberado para prosseguir. Aguardando " + $tempoEsperaPromax1+" segundos...")
                LogDB $revenda $logdb
                New-Item ($revenda.unit_disk+":\"+$revenda.unit_name+"\Promax\database\promax_backup.run") -type file
                Start-Sleep -Seconds $tempoEsperaPromax1
            }else{
                Write-Host ("New-Item "+ $revenda.unit_disk + ":\" +$revenda.unit_name+"\Promax\database\promax_backup.run -type file")
                write-Host Start-Sleep -Seconds $tempoEsperaPromax1
            }
            Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Verificando novamente se há processos criticos no Promax  " + $revenda.unit_name)
            $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Verificando novamente se há processos criticos no Promax  " + $revenda.unit_name)
            LogDB $revenda $logdb
            if(VerificaPromaxLiberado $revenda){
                if ($executa){
                    Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Backup liberado para prosseguir. Aguardando " + $tempoEsperaPromax2+" segundos...")
                    $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Backup liberado para prosseguir. Aguardando " + $tempoEsperaPromax2+" segundos...")
                    LogDB $revenda $logdb
                    Start-Sleep -Seconds $tempoEsperaPromax2
                }else{
                    Write-Host Start-Sleep -Seconds $tempoEsperaPromax2
                  }

                $comandosBKP = @()
                $podeExecutarBackup=$true
                $tipo = $revenda.type_name
     
                Foreach ($caminho in $diretorios) {	
                    if($tipo -eq $caminho.TipoBackup){
                        if($caminho.Tipo -eq "dir"){
                            $dirOrigem = $revenda.unit_disk+":\"+$revenda.unit_name+"\"+$caminho.Caminho
                            $dirDestino = $dirDestinoBase  +$revenda.unit_name +"\" +$caminho.Caminho
                            CheckPath $dirDestino $true
                            if($revendaManual -ne "" -and $tipoManual -ne ""){
                                $cmdRobocopy = " /TS /E /NP /NC /NJH /R:2 /W:2 /bytes /TEE /NFL"
                            }else{
                                $cmdRobocopy = " /TS /E /NP /NC /NJH /R:2 /W:2 /bytes /TEE /NFL /log+:"+$logFile   
                            }
                                if($tipo -eq "DIFF"){
                                    $cmdRobocopy += " /M"
                                }
                            $comandosBKP +="$dirOrigem $dirDestino" + $cmdRobocopy.split(' ')
                        }elseif($caminho.Tipo -eq "file"){
                            $dirOrigem = $revenda.unit_disk+":\"+$revenda.unit_name+"\"+ (Split-Path -Path $caminho.Caminho)
                            $dirDestino = $dirDestinoBase  +$revenda.unit_name +"\"+ (Split-Path -Path $caminho.Caminho)
                            $arquivo = (Split-Path -Path $caminho.Caminho -Leaf)
                            if(CheckPath $dirOrigem\$arquivo){

                                 $cmdRobocopy = " /TS /NP /NC /NJH /R:2 /W:2 /bytes /TEE /NFL /log+:"+$logFile   
                                    if($tipo -eq "diff"){
                                        $cmdRobocopy += " /M"
                                    }
                                    $comandosBKP +="$dirOrigem $dirDestino $arquivo" + $cmdRobocopy.split(' ')
                                }else{ 
                                    Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Arquivo " + $arquivo + " não existe.")
                                    $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Arquivo " + $arquivo + " não existe.")
                                    LogDB $revenda $logdb
                                }
                            }
                    }
                }
   
                  if($podeExecutarBackup){
                    $revenda.Diretorio = $dirDestinoBase +$revenda.unit_name
                    PararServicos $revenda
                    
     #INICIO DA CÓPIA"
      RegisterBackupStartCopy $revenda
                    foreach($comando in $comandosBKP){
                        if ($executa){
                            if($revendaManual -ne "" -and $tipoManual -ne ""){
                                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Executando copia dos arquivos: " + $comando.split(' ')[0])
                                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Executando copia dos arquivos: " + $comando.split(' ')[0]) 
                                LogDB $revenda $logdb
                            }else{
						        Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Executando copia dos arquivos: " + $comando.split(' ')[0])
                                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Executando copia dos arquivos: " + $comando.split(' ')[0])
                                LogDB $revenda $log
                                Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+ " " + $comando )
                            }

                            robocopy $comando.split(' ')
                            if($lastExitCode -gt 7){
                                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Executando Comando. Veja log para detalhes. robocopy " + $comando )
                                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Erro Executando Comando. Veja log para detalhes. robocopy " + $comando )
                                LogDB $revenda $logdb
                                $logzabbix = '12'
                                LogZabbix $revenda $logzabbix
                                LogAllTime $revenda $logzabbix 
                                #return $false
                            }else{
                    
                                #return $true
                            }
                        }else{
                            $cmd = "robocopy "+ $comando
                            Write-Host $cmd
                        }

                    }
            #FINAL DA CÓPIA
           RegisterBackupEndCopy $revenda 

                    if($revenda.type_name -eq "DIFF"){
                        if($executa){
                            IncrementaSeqBackup $revenda
                        }else{
                            Write-Host IncrementaSeqBackup $revenda
                        }
                    }else{
                        if($executa){
                            ZeraSeqBackup $revenda
                        }else{
                            Write-Host ZeraSeqBackup $revenda
                        }
                        $revenda.Sequencia="0"
                    }
                    $seqBackup = LerSeqBackup $revenda
                    IncrementaComp $revenda
                    CriarEventoCompactacao $revenda $seqBackup
                    if($executa){
                        Remove-Item ($revenda.unit_disk+":\"+$revenda.unit_name+"\Promax\database\promax_backup.run") -Force
                    }else{
                        Write-Host ("Remove-Item "+$revenda.unit_disk+":\"+$revenda.unit_name+"\Promax\database\promax_backup.run -Force")
                    }
                    IniciarServicos $revenda
                    $revenda.Fim = Get-Date -Format "dd/MM/yyyy HH:mm:ss"
                    $revenda.status = "ok"
                    $logzabbix = '0'
                    LogZabbix $revenda $logzabbix
                    LogAllTime $revenda $logzabbix 
                    Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" BACKUP FINALIZADO " + $revenda.unit_name + " - "+ $revenda.type_name)
                    $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" BACKUP FINALIZADO " + $revenda.unit_name + " - "+ $revenda.type_name)
                    LogDB $revenda $logdb
                    return $true
                }else{
                    $revenda.status = "waiting"
                    return $false
                }
            }else{
                if($executa){
                    Remove-Item ($revenda.unit_disk+":\"+$revenda.+"\Promax\database\promax_backup.run") -Force
                    Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Promax "+ $revenda.unit_name +" não está pronto para backup: " + $msgPromax + " - Aguardando..." )
                    $logdb =  ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Promax "+ $revenda.unit_name +" não está pronto para backup: " + $msgPromax + " - Aguardando..." )
                    LogDB $revenda $logdb
                    $revenda.status = "waiting"
                }else{
                    Write-Host ("Remove-Item "+$revenda.Disco+":\"+$revenda.Nome+"\Promax\database\promax_backup.run -Force")
                    Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Promax "+ $revenda.unit_name +" não está pronto para backup: " + $msgPromax + " - Aguardando...")
                    $logdb =  ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Promax "+ $revenda.unit_name +" não está pronto para backup: " + $msgPromax + " - Aguardando...")
                    LogDB $revenda $logdb 
                    Write-Host $revenda.status = "waiting"
                }
                return $false
            }
        }else{
            if($executa){
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Promax "+ $revenda.unit_name +" não está pronto para backup: " + $msgPromax + " - Aguardando...")
                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Promax "+ $revenda.unit_name +" não está pronto para backup: " + $msgPromax + " - Aguardando...")
                 LogDB $revenda $logdb 
                $revenda.status = "waiting"
            }else{
                Add-contentRevenda $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Promax "+ $revenda.unit_name +" não está pronto para backup: " + $msgPromax + " - Aguardando...")
                $logdb = ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Promax "+ $revenda.unit_name +" não está pronto para backup: " + $msgPromax + " - Aguardando...")
                LogDB $revenda $logdb 
                write-host $revenda.status = "waiting"
            }
            return $false
        }
    }
                 
}
#endregion

#region IncrementaComp
Function IncrementaComp(){
    param( 
        $revenda
        )
    $caminho = $dirArquivosCSV+"index\"+$revenda.unit_name+"_Comp"
    
    $stream = [System.IO.StreamWriter] $caminho
    $stream.Write(0)
    $stream.Close()  
}
#endregion

#region execução do backup

IniciaLog
AdicionaInfo
$result = ConnectSQL

#checa se a conexão está aberta
if($result -eq "Open"){
    $revendas = QuerySQL #antigo revendas.csv - contém apenas os backups do dia 
    Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" O backup será executado pelo banco de dados.")
 
   }else{
    if ($weekday -eq "0"){
        $revendas =  Get-Content  "W:\BACKUP\scripts\cachedomingo.json" | ConvertFrom-Json
        Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" O backup será executado pelo cache (cachedomingo.json)")
        Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" ALERTA: o arquivo de cache não será atualizado com os dados do dia seguinte.")
    }else{
        $revendas =  Get-Content  "W:\BACKUP\scripts\revendas.json" | ConvertFrom-Json
        Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" O backup será executado pelo cache (revendas.json).")
        Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" ALERTA: o arquivo de cache não será atualizado com os dados do dia seguinte. Necessário verificar a conexão com o banco de dados")
    }
}



if($revendaManual -ne "" -and $tipoManual -ne ""){
    Foreach ($revenda in $revendas) {
      if($weekday -eq '0'){
      $revenda.type_name= "DIFF"
      }
        if($revendaManual -eq $revenda.unit_name -and $tipoManual -eq "DIFF"){
            $revenda.backup_hour = Get-Date
             $logzabbix = 13;
             LogZabbix $revenda $logzabbix 
             InsertLogAllTime $revenda $logzabbix 
             BackupRevenda $revenda
      if($revenda.status -eq "waiting"){
             $revendaAguardando = $true
        }

          while($revendaAguardando){
            $revendaAguardando = $false
            $horaExec = [datetime] $revenda.backup_hour #hora de execução
            $weekday = (Get-Date).DayOfWeek.value__ #dia da 
            $horaAtual=Get-Date
            $horaTolerancia=($horaExec).AddMinutes($revenda.unit_tolerance)

             if($horaAtual -gt $horaExec -and $horaAtual -lt $horaTolerancia){
               if(-Not (BackupRevenda $revenda)){
                   $revendaAguardando = $true
                  }
           }elseif($horaAtual -gt $horaTolerancia) {
                Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Revenda " + $revenda.unit_name + " fora do horário permitido. Horário: " + $revenda.backup_hour + " tolerancia: "+ $revenda.unit_tolerance)
                $revenda.status = "failed"
                  $logzabbix = '14'
                  LogZabbix $revenda $logzabbix
                  LogAllTime $revenda $logzabbix 
            }else{
                 $revenda.status = "failed"
                 $logzabbix = '14'
                 LogZabbix $revenda $logzabbix
                 LogAllTime $revenda $logzabbix 
            }
            
            Start-Sleep -Seconds $tempoWaiting
        }
         break
        }
       }
      }else{

  #1ª rodada - sem loop
  $revendas = $revendas | Where-Object {$_.backup_hour -gt ((get-date).AddMinutes(-180)).ToString("HH:mm") -and $_.backup_hour -lt ((get-date).AddMinutes(180)).ToString("HH:mm")}
    Foreach ($revenda in $revendas) {
     InsertLogAllTime $revenda $logzabbix 

    if($weekday -eq '0'){
    $revenda.unit_tolerance = 180
    }
        $horaExec = [datetime] $revenda.backup_hour #hora de execução
        $weekday = (Get-Date).DayOfWeek.value__ #dia da semanada
        $horaAtual=Get-Date
        $horaTolerancia=($horaExec).AddMinutes($revenda.unit_tolerance)
        $logzabbix = 13;
        LogZabbix $revenda $logzabbix 
        LogAllTime $revenda $logzabbix 

    if($revenda.day_id -eq $weekday ){
        if($horaAtual -gt $horaExec -and $horaAtual -lt $horaTolerancia){
          BackupRevenda $revenda
    }else{
        if($horaExec -gt $horaAtual -and (NEW-TIMESPAN –Start $horaAtual  -End $horaExec ).TotalMinutes -lt $tempoEspera ){
           $revenda.status = "waiting"
        }else{
     if($horaAtual -gt $horaTolerancia) {        
      Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Revenda " + $revenda.unit_name + " - " + $revenda.type_name + " fora do horário permitido. Horário: " + $revenda.backup_hour + " tolerancia: "+ $revenda.unit_tolerance)
        $revenda.status = "failed"
         $logzabbix = '15'
         LogZabbix $revenda $logzabbix
         LogAllTime $revenda $logzabbix 
    }else{  
        $revenda.status = "failed"
        $logzabbix = '15'
        LogZabbix $revenda $logzabbix
        LogAllTime $revenda $logzabbix 
                }
            }
            if($executa){
                Start-Sleep -Seconds $tempoLoop
            }else{
                Write-Host Start-Sleep -Seconds $tempoLoop
            }
           }
          }
        }

#2ª rodada - Loop até terminar, se houver revendas que ainda não chegou no horário
    $revendaAguardando = $false
    Foreach ($revenda in $revendas) {
        if($revenda.status-eq "waiting"){
            $revendaAguardando = $true
        }
    }
    

    while($revendaAguardando){
     AtualizaHorarios
     $revendaAguardando = $false
         Foreach ($revenda in $revendas) {
           if($revenda.status -eq "waiting"){

            $horaExec = [datetime] $revenda.backup_hour 
            $weekday = (Get-Date).DayOfWeek.value__ 
            $horaAtual=Get-Date
            $horaTolerancia=($horaExec).AddMinutes($revenda.unit_tolerance)

            if($revenda.day_id -eq $weekday ){
            if($horaAtual -gt $horaExec -and $horaAtual -lt $horaTolerancia  ){
             
              if(-Not (BackupRevenda $revenda)){
                         write-host $revenda.unit_name
                         $revendaAguardando = $true
                    }
                }elseif($horaExec -gt $horaAtual -and (NEW-TIMESPAN –Start $horaAtual  -End $horaExec ).TotalMinutes -lt $tempoEspera ) {
                        Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Revenda " + $revenda.unit_name + " - " + $revenda.type_name + ": Execução antes do horário permitido. Aguardando... Horário: " + $revenda.backup_hour + " tolerancia: "+ $revenda.unit_tolerance)
                        $revenda.status = "waiting"
                        $revendaAguardando = $true
         
                    }elseif($horaAtual -gt $horaTolerancia) {
                        Add-content $logFile -value ((Get-Date -Format "dd/MM/yyyy HH:mm:ss")+" Revenda " + $revenda.unit_name + " - " + $revenda.type_name + ": fora do horário permitido. Horário: " + $revenda.backup_hour + " tolerancia: "+ $revenda.tolerance)
                        $revenda.status = "failed"
                         $logzabbix = '15'
                         LogZabbix $revenda $logzabbix
                         LogAllTime $revenda $logzabbix 
                    }else{
                        $revenda.status = "failed"
                        $logzabbix = '15'
                        LogZabbix $revenda $logzabbix
                        LogAllTime $revenda $logzabbix 
                    }
                }
                Start-Sleep -Seconds $tempoWaiting
            }
        }   
    }

    #Atualiza o cache
    CacheJson
	ImprimeResumo
}
#endregion



