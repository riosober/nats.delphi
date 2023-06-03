{ ****************************************************************************** }
{ }
{ NATS.Delphi: Delphi Client Library for NATS }
{ Copyright (c) 2022 Paolo Rossi }
{ https://github.com/paolo-rossi/nats.delphi }
{ }
{ ****************************************************************************** }
{ }
{ Licensed under the Apache License, Version 2.0 (the "License"); }
{ you may not use this file except in compliance with the License. }
{ You may obtain a copy of the License at }
{ }
{ http://www.apache.org/licenses/LICENSE-2.0 }
{ }
{ Unless required by applicable law or agreed to in writing, software }
{ distributed under the License is distributed on an "AS IS" BASIS, }
{ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. }
{ See the License for the specific language governing permissions and }
{ limitations under the License. }
{ }
{ ****************************************************************************** }
unit Demo.Form.Main;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  System.Generics.Collections, Vcl.Controls, Vcl.Forms, Vcl.Dialogs, IdBaseComponent, IdComponent, IdTCPConnection,
  IdTCPClient, Vcl.StdCtrls, Vcl.ExtCtrls, Demo.Form.Connection, Nats.Consts, Nats.Entities, Nats.Connection,
  Vcl.ComCtrls, Vcl.Imaging.pngimage, NATS.Wrapper;

type
  TfrmMain = class(TForm)
    memoLog: TMemo;
    pnlNetwork: TPanel;
    lstNetwork: TListBox;
    btnNewConnection: TButton;
    pnlClient: TPanel;
    Splitter1: TSplitter;
    pgcConnections: TPageControl;
    tsAbout: TTabSheet;
    imgNatsDelphi: TImage;
    Button1: TButton;
    Button2: TButton;
    Timer1: TTimer;
    Button3: TButton;
    procedure FormCreate(Sender: TObject);
    procedure btnNewConnectionClick(Sender: TObject);
    procedure Button1Click(Sender: TObject);
    procedure Button2Click(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure Timer1Timer(Sender: TObject);
    procedure Button3Click(Sender: TObject);
  private
    FCommands: TArrayNATSCommander;
    FIndexMsg: Integer;
  public
    procedure OnCommand(CONST cmd: TNATSCommander);
  end;

var
  frmMain: TfrmMain;

implementation

uses
  Nats.Classes,
  Nats.Parser;

{$R *.dfm}

procedure TfrmMain.Button1Click(Sender: TObject);
begin
  SingletonNATSObject.Subscribe('echo.reply', OnCommand);
end;

procedure TfrmMain.Button2Click(Sender: TObject);
begin
  SingletonNATSObject.Publish('echo', 'hello', '{"a":1,"b":"string"}')
end;

procedure TfrmMain.Button3Click(Sender: TObject);
begin
  SingletonNATSObject := TBizMQProcessor.Create('127.0.0.1', 4222, 'reel', 'deepleo');
end;

procedure TfrmMain.FormCreate(Sender: TObject);
begin
  FIndexMsg := 0;
  Color := RGB(Random(255), Random(255), Random(255));
  FCommands := TArrayNATSCommander.Create;
end;

procedure TfrmMain.FormDestroy(Sender: TObject);
begin
  FreeAndNil(FCommands);
end;

procedure TfrmMain.OnCommand(CONST cmd: TNATSCommander);
begin
  System.TMonitor.Enter(FCommands);
  try
    FCommands.Add(cmd);
  finally
    System.TMonitor.Exit(FCommands);
  end;
end;

procedure TfrmMain.Timer1Timer(Sender: TObject);
var
  i: Integer;
  cmd: TNATSCommander;
begin
  System.TMonitor.Enter(FCommands);
  try
    for i := FCommands.Count - 1 downto 0 do
    begin
      cmd := FCommands.Items[i];
      FCommands.Remove(cmd);
      memoLog.Lines.Add(cmd.ToJSONString)
    end;
  finally
    System.TMonitor.Exit(FCommands);
  end;
  inc(FIndexMsg);
  SingletonNATSObject.Publish('echo', 'hello'+IntToStr(FIndexMsg), '{"a":1,"b":"string"}')
end;

procedure TfrmMain.btnNewConnectionClick(Sender: TObject);
var
  LConn: TfrmConnection;
  LTabSheet: TTabSheet;
begin
  LTabSheet := TTabSheet.Create(pgcConnections);
  LTabSheet.Caption := 'Connection ' + pgcConnections.PageCount.ToString;
  LTabSheet.PageControl := pgcConnections;

  LConn := TfrmConnection.CreateAndShow(LTabSheet.Caption, LTabSheet, memoLog.Lines);
  lstNetwork.AddItem(LTabSheet.Caption, LConn.Connection);
  pgcConnections.ActivePage := LTabSheet;
end;

initialization

Randomize;

end.
