#define AppName "Independent Download Manager"
#define AppVersion "0.1.0"
#define AppPublisher "Independent Download Manager"
#define AppExeName "independent-download-manager.exe"

[Setup]
AppId={{A9451DB6-B006-4E3D-ACD8-DF7DF00408AB}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName={code:DefaultInstallDir}
DefaultGroupName={#AppName}
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64
OutputDir=.
OutputBaseFilename=setup-independent-download-manager
UninstallDisplayIcon={app}\{#AppExeName}
Compression=lzma2
SolidCompression=yes
WizardStyle=modern

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; GroupDescription: "Additional shortcuts:"; Flags: unchecked

[Files]
Source: "target\x86_64-pc-windows-msvc\release\{#AppExeName}"; DestDir: "{app}"; Flags: ignoreversion
Source: "extension\*"; DestDir: "{app}\extension"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\{#AppName}"; Filename: "{app}\{#AppExeName}"
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExeName}"; Tasks: desktopicon

[Registry]
; HKA selects HKCU for the default per-user install or HKLM for the optional
; all-users install selected in Inno Setup's privilege override dialog.
Root: HKA; Subkey: "Software\Google\Chrome\NativeMessagingHosts\org.independent.download_manager"; ValueType: string; ValueName: ""; ValueData: "{app}\native-host.json"; Flags: uninsdeletekey

[UninstallDelete]
Type: files; Name: "{app}\native-host.json"

[Code]
var
  ExtensionIdPage: TInputQueryWizardPage;

procedure InitializeWizard;
begin
  ExtensionIdPage := CreateInputQueryPage(wpSelectDir, 'Chrome integration',
    'Optional Chrome Manifest V3 integration',
    'Leave the extension ID blank if you are not configuring Chrome now.');
  ExtensionIdPage.Add('Chrome extension ID (32 characters a-p):', False);
end;

function IsValidExtensionId(const Value: String): Boolean;
var
  Index: Integer;
begin
  Result := Length(Value) = 32;
  if not Result then Exit;
  for Index := 1 to Length(Value) do begin
    if (Value[Index] < 'a') or (Value[Index] > 'p') then begin
      Result := False;
      Exit;
    end;
  end;
end;

function NextButtonClick(CurPageID: Integer): Boolean;
var
  ExtensionId: String;
begin
  Result := True;
  if CurPageID = ExtensionIdPage.ID then begin
    ExtensionId := Trim(ExtensionIdPage.Values[0]);
    if (ExtensionId <> '') and not IsValidExtensionId(ExtensionId) then begin
      MsgBox('Enter a Chrome extension ID containing exactly 32 characters from a through p, or leave it blank.', mbError, MB_OK);
      Result := False;
    end;
  end;
end;

function DefaultInstallDir(Param: String): String;
begin
  if IsAdminInstallMode then
    Result := ExpandConstant('{autopf}\Independent Download Manager')
  else
    Result := ExpandConstant('{localappdata}\Programs\Independent Download Manager');
end;

function JsonEscape(Value: String): String;
begin
  Result := Value;
  StringChangeEx(Result, '\', '\\', True);
  StringChangeEx(Result, '"', '\"', True);
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  ManifestText: String;
  ExePath: String;
  ExtensionId: String;
begin
  if CurStep = ssPostInstall then begin
    ExePath := JsonEscape(ExpandConstant('{app}\{#AppExeName}'));
    ExtensionId := Trim(ExtensionIdPage.Values[0]);
    if ExtensionId = '' then ExtensionId := 'REPLACE_WITH_EXTENSION_ID';
    ManifestText := '{"name":"org.independent.download_manager","description":"Independent Download Manager Native Messaging host","path":"' + ExePath + '","type":"stdio","args":["--native-host"],"allowed_origins":["chrome-extension://' + ExtensionId + '/"]}';
    if not SaveStringToFile(ExpandConstant('{app}\native-host.json'), ManifestText, False) then
      RaiseException('Could not write the Chrome Native Messaging host manifest.');
  end;
end;
