from flet import * 

def main(page: Page):
  page.title = "Acesso"
  
  login = Container(
    width=320,
    height=750,
    bgcolor="#ffffff",
    border_radius=10,
    content=Column(     
      width=320,
      controls=[
        Container(
          width=300,
          margin=margin.only(left=200,right=10,top=10),
          content=TextButton(
            "Criar acesso",
            style=ButtonStyle(
              color="#000000"
            )
          )
        ),
        Container(
          width=300,
          margin=margin.only(left=20,right=10,top=40),
          content=Text(
            "Acesso ao Ambiente",
            size=30,
            color="#000000",
            weight="w700"
          )
        ),
        Container(
          width=300,
          margin=margin.only(left=10,right=10,top=10),
          alignment=alignment.center,
          content=Text(
            "Por favor informe suas credenciais de acesso",
            size=12,
            color="#000000",
            text_align="center"
          ),
        ),
        Container(
          width=300,
          margin=margin.only(left=20,right=20,top=35),
          content=Column(
            controls=[
              Text(
                "e-Mail",
                size=14,
                color="#000000"
              ),
              TextField(
                text_style=TextStyle(
                  color="#000000"
                ),
                border_radius=15,
                border_color=colors.BLACK,
                focused_border_color=colors.ORANGE_700,
              ),
            ],
          )
        ),
        Container(
          width=300,
          margin=margin.only(left=20,right=20,top=5),
          content=Column(
            controls=[
              Text(
                "Senha",
                size=14,
                color="#000000"
              ),
              TextField(
                text_style=TextStyle(
                  color="#000000"
                ),
                password=True,
                can_reveal_password=True,
                border_radius=15,
                border_color=colors.BLACK,
                focused_border_color=colors.ORANGE_700,
              ),
            ],
          )
        ),
        Container(
          width=300,
          margin=margin.only(left=120),
          content=TextButton(
            "Esqueceu a senha?",
            style=ButtonStyle(
              color="#000000"
            )
          ),
        ),
        Container(
          width=300,
          margin=margin.only(left=20,right=20,top=20),
          content=TextButton(
            "Acessar",
            width=300,
            style=ButtonStyle(
              color="#ffffff",
              bgcolor=colors.ORANGE_700,
              shape={
                MaterialState.FOCUSED: RoundedRectangleBorder(radius=5),
                MaterialState.HOVERED: RoundedRectangleBorder(radius=5),
              },
              padding=30,
            ),
          ),
        ),
        Container(
          width=300,
          margin=margin.only(left=20,right=20,top=20),
          content=Text(
            
          )
        )

      ]   
                   
    ),
  )
  
  signup = Container(
    width=320,
    height=750,
    bgcolor="#ffffff",
    border_radius=10,
    content=Column(      
      width=320,
      controls=[
        Container(
          width=40,
          height=40,
          border_radius=10,
          margin=margin.only(left=10,top=10),
          content=IconButton(
            icon_color="#000000",
            icon=icons.ARROW_BACK_IOS_NEW_OUTLINED,
            style=ButtonStyle(
              side={
                MaterialState.DEFAULT: border.BorderSide(1,colors.BLACK)
              },
            )
          )
        )
      ]                 
    ),    
  )
  
  body = Container(
    width=1000,
    height=800,
    content=Row(
      controls=[
        login,
        signup
      ]
    )
  )
  
  page.add(body)
  
app(target=main)